package main

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	pb "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	publicmanifest "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginsdk/manifest"
	sdkruntime "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginsdk/runtime"
	"github.com/Silo-Server/silo-plugin-sdk/pkg/pluginsdk/runtimedefault"
	"github.com/Silo-Server/silo-plugin-sdk/pkg/pluginsdk/runtimehost"
	"github.com/drondeseries/silo-virtual-library/pkg/release"
	"github.com/hashicorp/go-hclog"
)

const (
	virtualPathPrefix        = "virtual://"
	configKey                = "streaming"
	maxResponseBytes         = 4 << 20
	defaultCacheTTLMinutes   = 10
	minCacheTTLMinutes       = 1
	maxCacheTTLMinutes       = 10080
	maxCandidateCacheEntries = 256
	maxCandidateCacheBytes   = 16 << 20
	maxVirtualCandidates     = 50
	maxManifestResponseBytes = 256 << 10
	// negativeCacheTTL bounds how long an empty provider answer is reused.
	// Without it, a title the provider cannot serve re-paid a full 3-8s
	// round-trip on every resolve — four times inside one playback start in
	// production. Entries expire so newly added sources are noticed without
	// operator action.
	negativeCacheTTL = 2 * time.Minute
	// freshServeFloor caps how aggressively forced lookups re-fetch. One
	// playback start walks several candidate rounds; serving results younger
	// than this floor even to GetCandidatesFresh keeps an entire attempt at
	// one provider round-trip while bounding staleness well under the TTL.
	freshServeFloor = 30 * time.Second
	// candidateStaleGrace extends candidate-cache usefulness past its TTL:
	// an expired-but-non-empty entry is served immediately while one
	// background refresh repopulates it. Provider URLs are short-lived
	// tokens whose actual lifetime is unknown to the plugin, so the grace is
	// intentionally short — long enough to cover a typical playback start,
	// short enough that expired credentials are not served for long.
	candidateStaleGrace      = 3 * time.Minute
	backgroundRefreshTimeout = 15 * time.Second
)

//go:embed manifest.json
var manifestJSON []byte

// resolvePluginDataPath turns a possibly-relative state file path into an
// absolute path under the plugin data directory so monitor state survives
// container recreation. The legacy default wrote into the process cwd, which
// is ephemeral on the host. An existing relative file is migrated once.
func resolvePluginDataPath(file, fallback string) string {
	file = strings.TrimSpace(file)
	if file == "" {
		file = fallback
	}
	if filepath.IsAbs(file) {
		return file
	}
	base := strings.TrimSpace(os.Getenv("SILO_PLUGIN_CACHE_DIR"))
	if base == "" {
		return file
	}
	dir := filepath.Join(base, "com.drondeseries.silo-virtual-library")
	target := filepath.Join(dir, file)
	if _, err := os.Stat(target); err == nil {
		return target
	}
	if _, err := os.Stat(file); err == nil {
		_ = os.MkdirAll(dir, 0o700)
		if data, readErr := os.ReadFile(file); readErr == nil {
			_ = os.WriteFile(target, data, 0o600)
		}
	}
	return target
}

type streamResolver interface {
	Resolve(context.Context, string) (string, error)
	GetVariants(context.Context, string) []runtimehost.VirtualMediaVariant
}

type candidateLister interface {
	GetCandidates(context.Context, string) ([]StreamCandidate, string, string, error)
}
type resolverConfig struct {
	ManifestURL            string
	AllowInsecure          bool
	Quality                QualityConfig
	CacheTTLMinutes        int
	TMDBAPIKey             string
	IndexerRSSURL          string
	IndexerAPIKey          string
	IndexerRSSCheckMinutes int
}
type manifestStreamResolver struct {
	client          *http.Client
	mu              sync.RWMutex
	config          resolverConfig
	generation      uint64
	cacheMu         sync.Mutex
	cache           map[string]candidateCacheEntry
	cacheGeneration uint64
	cacheBytes      int64
	refreshes       map[string]chan struct{}
	syncFlights     map[string]chan struct{}
	releaseStore    *release.ReleaseStore
	logger          hclog.Logger
}

// unreleasedError reports that tracked release metadata places the requested
// movie or episode in the future. It is never surfaced as a playable
// candidate: the host selects candidates by rank without consulting
// availability flags, so any placeholder source would be handed to the
// player and fail at stream-open.
type unreleasedError struct {
	message string
}

func (e *unreleasedError) Error() string { return e.message }

func newUnreleasedError(imdbID string, airDate *time.Time, itemType string) *unreleasedError {
	formatted := "soon"
	if airDate != nil && !airDate.IsZero() {
		formatted = airDate.UTC().Format("2006-01-02 15:04 MST")
	}
	noun := "episode"
	if strings.EqualFold(itemType, "movie") {
		noun = "movie"
	}
	return &unreleasedError{message: fmt.Sprintf("This %s (%s) airs %s. Streams appear automatically once it is released.", noun, imdbID, formatted)}
}

type candidateCacheEntry struct {
	candidates []StreamCandidate
	expiresAt  time.Time
	// fetchedAt records when the provider actually answered, distinct from
	// lastAccess which moves on every serve. The forced-lookup floor is
	// judged against fetchedAt so repeated resolves inside one playback
	// start stay on one round-trip.
	fetchedAt  time.Time
	lastAccess time.Time
	sizeBytes  int64
}

type stremioResponse struct {
	Streams []StreamCandidate `json:"streams"`
}

type stremioManifest struct {
	ID        string            `json:"id"`
	Resources []json.RawMessage `json:"resources"`
	Types     []string          `json:"types"`
}

func (c *manifestStreamResolver) Configure(config resolverConfig) {
	if config.CacheTTLMinutes == 0 {
		config.CacheTTLMinutes = defaultCacheTTLMinutes
	}
	if config.CacheTTLMinutes < minCacheTTLMinutes {
		config.CacheTTLMinutes = minCacheTTLMinutes
	}
	if config.CacheTTLMinutes > maxCacheTTLMinutes {
		config.CacheTTLMinutes = maxCacheTTLMinutes
	}
	c.mu.Lock()
	c.config = config
	c.generation++
	generation := c.generation
	c.mu.Unlock()
	c.cacheMu.Lock()
	c.cache = nil
	c.cacheBytes = 0
	c.cacheGeneration = generation
	c.cacheMu.Unlock()
}

func cloneCandidates(candidates []StreamCandidate) []StreamCandidate {
	return append([]StreamCandidate(nil), candidates...)
}

func (c *manifestStreamResolver) Resolve(ctx context.Context, virtualPath string) (string, error) {
	candidates, _, _, err := c.GetCandidates(ctx, virtualPath)
	if err != nil {
		return "", err
	}
	if len(candidates) == 0 {
		return "", errors.New("streaming provider returned no streams")
	}

	selected := c.SelectCandidates(virtualPath, candidates)
	if len(selected) == 0 {
		u, _ := url.Parse(virtualPath)
		if u != nil && strings.TrimSpace(u.Query().Get("profile")) != "" {
			return "", fmt.Errorf("no stream matches profile %q", u.Query().Get("profile"))
		}
		return "", errors.New("no stream matches the requested selection")
	}
	return selected[0].URL, nil
}

// SelectCandidates applies the quality-profile policy while retaining every
// ranked candidate so the host can fail over when a temporary provider URL
// fails. Catalog variants remain one per configured profile label.
func (c *manifestStreamResolver) SelectCandidates(virtualPath string, candidates []StreamCandidate) []StreamCandidate {
	u, _ := url.Parse(virtualPath)
	if u == nil {
		return cloneCandidates(candidates)
	}
	requestedProfile := strings.TrimSpace(u.Query().Get("profile"))
	requestedResult := strings.TrimSpace(u.Query().Get("result"))
	c.mu.RLock()
	config := c.config.Quality
	c.mu.RUnlock()

	var profile QualityProfile
	if requestedProfile != "" && config.EnableProfiles {
		profile = profileByLabel(config.Profiles, requestedProfile)
		profileFound := false
		for _, configured := range config.Profiles {
			if strings.EqualFold(configured.Label, requestedProfile) {
				profileFound = true
				break
			}
		}
		if !profileFound {
			if !config.FallbackToAnyStream {
				return nil
			}
			requestedProfile = ""
		}
	}
	if requestedProfile != "" && config.EnableProfiles {
		matched := make([]StreamCandidate, 0, len(candidates))
		for _, candidate := range candidates {
			if matchProfile(candidate, profile) {
				if _, rejected := customFormatScore(candidate, config.CustomFormats); !rejected {
					matched = append(matched, candidate)
				}
			}
		}
		if len(matched) > 0 {
			sortCandidatesForProfile(matched, profile, config.CustomFormats)
			if requestedResult != "" {
				for _, candidate := range matched {
					if candidateVariantID(candidate) == requestedResult {
						return []StreamCandidate{candidate}
					}
				}
				return nil
			}
			return matched
		}
		if !config.FallbackToAnyStream {
			return nil
		}
	}

	ranked := cloneCandidates(candidates)
	filtered := ranked[:0]
	for _, candidate := range ranked {
		if _, rejected := customFormatScore(candidate, config.CustomFormats); !rejected {
			filtered = append(filtered, candidate)
		}
	}
	ranked = filtered
	sortCandidatesForProfile(ranked, QualityProfile{}, config.CustomFormats)
	if requestedResult != "" {
		for _, candidate := range ranked {
			if candidateVariantID(candidate) == requestedResult {
				return []StreamCandidate{candidate}
			}
		}
		return nil
	}
	return ranked
}

func (c *manifestStreamResolver) GetCandidates(ctx context.Context, virtualPath string) ([]StreamCandidate, string, string, error) {
	return c.getCandidates(ctx, virtualPath, false)
}

// GetCandidatesFresh bypasses the bounded candidate cache for an explicit
// user refresh/retry while retaining the normal cache behavior by default.
func (c *manifestStreamResolver) GetCandidatesFresh(ctx context.Context, virtualPath string) ([]StreamCandidate, string, string, error) {
	return c.getCandidates(ctx, virtualPath, true)
}

func (c *manifestStreamResolver) getCandidates(ctx context.Context, virtualPath string, forceRefresh bool) ([]StreamCandidate, string, string, error) {
	mediaType, mediaID, err := parseVirtualPath(virtualPath)
	if err != nil {
		return nil, mediaType, mediaID, err
	}
	c.mu.RLock()
	config := c.config
	generation := c.generation
	releaseStore := c.releaseStore
	c.mu.RUnlock()

	if strings.Contains(virtualPath, "refresh=1") || strings.Contains(virtualPath, "force=1") {
		forceRefresh = true
	}
	// strip query from mediaID
	if idx := strings.Index(mediaID, "?"); idx != -1 {
		mediaID = mediaID[:idx]
	}
	// Silo keeps TVDB-based catalog IDs for stable series identity, but the
	// Stremio stream protocol expects IMDb video IDs (tt...:season:episode).
	// Translate legacy TVDB virtual paths before contacting the provider.
	if mediaType == "series" {
		mediaID, err = c.normalizeSeriesProviderID(ctx, mediaID)
		if err != nil {
			return nil, mediaType, mediaID, err
		}
	}
	if strings.HasPrefix(strings.ToLower(mediaID), "tmdb:") {
		mediaID, err = c.normalizeTMDBProviderID(ctx, mediaType, mediaID, config.TMDBAPIKey)
		if err != nil {
			return nil, mediaType, mediaID, err
		}
	}

	// Intercept unreleased media before contacting the streaming provider
	if releaseStore != nil {
		imdbID := mediaID
		season := 0
		episode := 0
		if mediaType == "series" {
			parts := strings.Split(mediaID, ":")
			if len(parts) >= 3 {
				imdbID = parts[0]
				season, _ = strconv.Atoi(parts[1])
				episode, _ = strconv.Atoi(parts[2])
			} else if len(parts) == 1 {
				imdbID = parts[0]
			}
		}
		if released, airDate := releaseStore.IsReleased(mediaType, imdbID, season, episode); !released {
			return nil, mediaType, mediaID, newUnreleasedError(imdbID, airDate, mediaType)
		}
	}
	cacheKey := mediaType + "|" + mediaID

	if forceRefresh {
		// Forced lookups still serve very recent answers. One playback start
		// walks several resolve rounds; re-fetching within a single attempt
		// multiplies provider latency without producing new information, so
		// entries younger than the floor are served as-is regardless of
		// emptiness. Anything older takes the full fetch path below.
		now := time.Now()
		c.cacheMu.Lock()
		entry, ok := c.cache[cacheKey]
		if sameGeneration := c.cacheGeneration == generation; sameGeneration && ok && now.Before(entry.fetchedAt.Add(freshServeFloor)) {
			candidates := cloneCandidates(entry.candidates)
			entry.lastAccess = now
			c.cache[cacheKey] = entry
			c.cacheMu.Unlock()
			return candidates, mediaType, mediaID, nil
		}
		c.cacheMu.Unlock()
	}

	// Cache tiers: fresh serve → stale-in-grace serve + one background
	// refresh → singleflight blocking fetch past grace or on force_refresh.
	if !forceRefresh {
		now := time.Now()
		c.cacheMu.Lock()
		entry, ok := c.cache[cacheKey]
		sameGeneration := c.cacheGeneration == generation
		switch {
		case sameGeneration && ok && now.Before(entry.expiresAt):
			candidates := cloneCandidates(entry.candidates)
			entry.lastAccess = now
			c.cache[cacheKey] = entry
			c.cacheMu.Unlock()
			return candidates, mediaType, mediaID, nil
		case sameGeneration && ok && len(entry.candidates) > 0 &&
			!now.Before(entry.expiresAt) && now.Before(entry.expiresAt.Add(candidateStaleGrace)):
			candidates := cloneCandidates(entry.candidates)
			entry.lastAccess = now
			c.cache[cacheKey] = entry
			started := c.startRefreshLocked(cacheKey, generation, config, mediaType, mediaID)
			c.cacheMu.Unlock()
			if started {
				c.debugLog("stale candidates served; background refresh started", cacheKey, len(candidates))
			} else {
				c.debugLog("stale candidates served; refresh already running", cacheKey, len(candidates))
			}
			return candidates, mediaType, mediaID, nil
		}
		c.cacheMu.Unlock()

		// Past stale grace: deduplicate concurrent blocking fetches through
		// the same keyed flight used for background refreshes. The first
		// caller blocks on the provider; later callers receive its result.
		if wait := c.joinFlight(cacheKey, config, generation, mediaType, mediaID); wait != nil {
			<-wait
			c.cacheMu.Lock()
			fresh, stillOK := c.cache[cacheKey]
			c.cacheMu.Unlock()
			if stillOK {
				now := time.Now()
				withinGrace := !now.After(fresh.expiresAt.Add(candidateStaleGrace))
				switch {
				case len(fresh.candidates) > 0 && withinGrace:
					// Positive results stay servable through the same stale
					// grace the direct tiers use.
					return cloneCandidates(fresh.candidates), mediaType, mediaID, nil
				case len(fresh.candidates) == 0 && now.Before(fresh.expiresAt):
					// Negative-cache hit: the flight already proved the title
					// is unavailable, so waiting callers must not re-pay the
					// round-trip. Negatives never outlive their own short TTL.
					return cloneCandidates(fresh.candidates), mediaType, mediaID, nil
				}
			}
			// Flight completed without a usable entry (expired, past grace,
			// or absent); fall through to our own attempt.
		}
	}

	candidates, err := c.fetchProviderCandidates(ctx, config, generation, cacheKey, mediaType, mediaID)
	return candidates, mediaType, mediaID, err
}

// joinFlight registers this caller as a synchronous provider fetcher if no
// other flight is active for the key. It returns a channel to wait on, or
// nil if this caller should proceed directly (first-in wins). Callers must
// NOT hold cacheMu.
func (c *manifestStreamResolver) joinFlight(cacheKey string, config resolverConfig, generation uint64, mediaType, mediaID string) <-chan struct{} {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()
	if c.refreshes == nil {
		c.refreshes = make(map[string]chan struct{})
	}
	if c.syncFlights == nil {
		c.syncFlights = make(map[string]chan struct{})
	}
	if _, inflight := c.refreshes[cacheKey]; inflight {
		ch, exists := c.syncFlights[cacheKey]
		if !exists {
			ch = make(chan struct{})
			c.syncFlights[cacheKey] = ch
		}
		return ch
	}
	done := make(chan struct{})
	c.refreshes[cacheKey] = done
	c.syncFlights[cacheKey] = done
	go func() {
		defer func() {
			c.cacheMu.Lock()
			delete(c.refreshes, cacheKey)
			delete(c.syncFlights, cacheKey)
			c.cacheMu.Unlock()
			close(done)
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		if _, err := c.fetchProviderCandidates(ctx, config, generation, cacheKey, mediaType, mediaID); err != nil {
			c.debugLog("synchronous candidate fetch failed", cacheKey, 0)
			return
		}
		c.debugLog("synchronous candidate fetch complete", cacheKey, 0)
	}()
	return done
}

// startRefreshLocked launches exactly one background provider fetch per
// cache key. Callers must hold cacheMu; the spawned goroutine replaces the
// cache entry through the normal generation-checked path.
func (c *manifestStreamResolver) startRefreshLocked(cacheKey string, generation uint64, config resolverConfig, mediaType, mediaID string) bool {
	if _, inflight := c.refreshes[cacheKey]; inflight {
		return false
	}
	if c.refreshes == nil {
		c.refreshes = make(map[string]chan struct{})
	}
	done := make(chan struct{})
	c.refreshes[cacheKey] = done
	go func() {
		defer func() {
			c.cacheMu.Lock()
			delete(c.refreshes, cacheKey)
			c.cacheMu.Unlock()
			close(done)
		}()
		ctx, cancel := context.WithTimeout(context.Background(), backgroundRefreshTimeout)
		defer cancel()
		if _, err := c.fetchProviderCandidates(ctx, config, generation, cacheKey, mediaType, mediaID); err != nil {
			c.debugLog("background candidate refresh failed", cacheKey, 0)
			return
		}
		c.debugLog("background candidate refresh complete", cacheKey, 0)
	}()
	return true
}

// fetchProviderCandidates performs a synchronous streaming-provider lookup
// and caches successful results. Provider URLs are never logged.
func (c *manifestStreamResolver) fetchProviderCandidates(ctx context.Context, config resolverConfig, generation uint64, cacheKey, mediaType, mediaID string) ([]StreamCandidate, error) {
	started := time.Now()
	endpoint, err := streamEndpointWithPolicy(config.ManifestURL, mediaType, mediaID, config.AllowInsecure)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("create streaming provider request: %w", err)
	}
	client := c.client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, errors.New("request streaming provider failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("streaming provider returned status %d", resp.StatusCode)
	}
	var payload stremioResponse
	if err := decodeBoundedJSON(resp.Body, maxResponseBytes, &payload); err != nil {
		return nil, fmt.Errorf("decode streaming provider response: %w", err)
	}
	validCandidates := make([]StreamCandidate, 0, len(payload.Streams))
	for i, stream := range payload.Streams {
		parsed, parseErr := url.Parse(strings.TrimSpace(stream.URL))
		if parseErr == nil && parsed.IsAbs() && (parsed.Scheme == "https" || parsed.Scheme == "http") {
			// Some providers answer unavailable titles with a placeholder
			// entry instead of an empty list. Persisting or ranking it makes
			// every start pay a doomed probe and pollutes the catalog with
			// ghost variants — drop it at ingestion.
			if isProviderStubCandidate(stream) {
				continue
			}
			stream.OriginalIndex = i
			parseStreamDetails(&stream)
			parseStreamMetadata(&stream)
			validCandidates = append(validCandidates, stream)
			if len(validCandidates) >= maxVirtualCandidates {
				break
			}
		}
	}
	ttlMinutes := config.CacheTTLMinutes
	if ttlMinutes == 0 {
		ttlMinutes = defaultCacheTTLMinutes
	}
	now := time.Now()
	c.storeCandidateCache(cacheKey, validCandidates, now.Add(time.Duration(ttlMinutes)*time.Minute), now, generation)
	if c.logger != nil {
		c.logger.Info("provider candidates fetched",
			"media_type", mediaType, "media_id", mediaID,
			"count", len(validCandidates),
			"duration_ms", time.Since(started).Milliseconds())
	}
	return validCandidates, nil
}

// isProviderStubCandidate reports whether a provider stream entry is the
// conventional "nothing found" placeholder rather than a playable source.
// Addons signal this via the entry's display text; the URL itself usually
// still looks plausible, so it must be checked before ingestion.
func isProviderStubCandidate(s StreamCandidate) bool {
	hay := strings.ToLower(strings.Join([]string{s.Name, s.Title, s.Description}, " \n "))
	for _, marker := range []string{
		"no streams available",
		"no streams found",
		"nothing found",
		"no results",
	} {
		if strings.Contains(hay, marker) {
			return true
		}
	}
	return false
}

func (c *manifestStreamResolver) debugLog(msg, cacheKey string, count int) {
	if c.logger == nil {
		return
	}
	c.logger.Debug(msg, "cache_key", cacheKey, "count", count)
}

func candidateCacheSize(candidates []StreamCandidate) int64 {
	var size int64
	for _, candidate := range candidates {
		size += 256
		for _, value := range []string{
			candidate.URL, candidate.Name, candidate.Title, candidate.Description,
			candidate.Resolution, candidate.CodecVideo, candidate.CodecAudio,
			candidate.HDR, candidate.SourceType, candidate.Container, candidate.BehaviorHints.VideoHash,
		} {
			size += int64(len(value))
		}
		for _, language := range candidate.AudioLanguages {
			size += int64(len(language))
		}
		for _, language := range candidate.SubtitleLanguages {
			size += int64(len(language))
		}
	}
	return size
}

func (c *manifestStreamResolver) storeCandidateCache(key string, candidates []StreamCandidate, expiresAt, now time.Time, generation uint64) {
	size := int64(0)
	if len(candidates) == 0 {
		// Negative caching: an empty provider answer is stored briefly so
		// repeated resolves of an unavailable title fail in microseconds
		// instead of paying another 3-8s round-trip each. The short TTL keeps
		// newly added sources discoverable without operator action.
		expiresAt = now.Add(negativeCacheTTL)
	} else {
		size = candidateCacheSize(candidates)
		if size > maxCandidateCacheBytes {
			return
		}
	}
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()
	if c.cacheGeneration != generation {
		return
	}
	if c.cache == nil {
		c.cache = make(map[string]candidateCacheEntry)
	}
	if previous, exists := c.cache[key]; exists {
		c.cacheBytes -= previous.sizeBytes
		delete(c.cache, key)
	}
	for candidateKey, entry := range c.cache {
		if !now.Before(entry.expiresAt) {
			c.cacheBytes -= entry.sizeBytes
			delete(c.cache, candidateKey)
		}
	}
	for len(c.cache) >= maxCandidateCacheEntries || c.cacheBytes+size > maxCandidateCacheBytes {
		oldestKey := ""
		var oldest time.Time
		for candidateKey, entry := range c.cache {
			if oldestKey == "" || entry.lastAccess.Before(oldest) {
				oldestKey, oldest = candidateKey, entry.lastAccess
			}
		}
		if oldestKey == "" {
			break
		}
		c.cacheBytes -= c.cache[oldestKey].sizeBytes
		delete(c.cache, oldestKey)
	}
	c.cache[key] = candidateCacheEntry{
		candidates: cloneCandidates(candidates),
		expiresAt:  expiresAt,
		fetchedAt:  now,
		lastAccess: now,
		sizeBytes:  size,
	}
	c.cacheBytes += size
}

func (c *manifestStreamResolver) normalizeTMDBProviderID(ctx context.Context, mediaType, mediaID, apiKey string) (string, error) {
	parts := strings.Split(mediaID, ":")
	if len(parts) < 2 || !strings.EqualFold(parts[0], "tmdb") {
		return mediaID, nil
	}
	key := strings.TrimSpace(apiKey)
	if key == "" {
		return "", errors.New("TMDB ID requires a configured TMDB API token to resolve IMDb playback ID")
	}
	externals, err := fetchTMDBExternalIDs(ctx, mediaType, parts[1], key)
	if err != nil || strings.TrimSpace(externals.IMDbID) == "" {
		return "", fmt.Errorf("TMDB ID %s has no IMDb playback ID", parts[1])
	}
	if len(parts) > 2 {
		return externals.IMDbID + ":" + strings.Join(parts[2:], ":"), nil
	}
	return externals.IMDbID, nil
}

func (c *manifestStreamResolver) cacheTTLSeconds() int64 {
	c.mu.RLock()
	minutes := c.config.CacheTTLMinutes
	c.mu.RUnlock()
	if minutes == 0 {
		minutes = defaultCacheTTLMinutes
	}
	return int64(minutes * 60)
}

func parseCacheTTLMinutes(value any) (int, error) {
	if value == nil {
		return defaultCacheTTLMinutes, nil
	}
	var minutes int
	switch v := value.(type) {
	case float64:
		minutes = int(v)
		if v != float64(minutes) {
			return 0, errors.New("cache_ttl_minutes must be an integer")
		}
	case int:
		minutes = v
	case int32:
		minutes = int(v)
	case int64:
		minutes = int(v)
	case string:
		parsed, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil {
			return 0, errors.New("cache_ttl_minutes must be an integer")
		}
		minutes = parsed
	default:
		return 0, errors.New("cache_ttl_minutes must be an integer")
	}
	if minutes < minCacheTTLMinutes || minutes > maxCacheTTLMinutes {
		return 0, fmt.Errorf("cache_ttl_minutes must be between %d and %d", minCacheTTLMinutes, maxCacheTTLMinutes)
	}
	return minutes, nil
}

func (c *manifestStreamResolver) ValidateConnection(ctx context.Context) error {
	c.mu.RLock()
	manifestURL := c.config.ManifestURL
	allowInsecure := c.config.AllowInsecure
	c.mu.RUnlock()
	if _, err := streamEndpointWithPolicy(manifestURL, "movie", "tt0000001", allowInsecure); err != nil {
		return err
	}
	// Use a short-timeout clone of the provider client so TestConnection
	// can't exhaust the SDK's gRPC deadline. Copy the transport (so mocks
	// and redirect policies still work) but cap the round-trip at 5s.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, manifestURL, nil)
	if err != nil {
		return fmt.Errorf("create manifest validation request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	var client *http.Client
	if c.client != nil {
		client = &http.Client{Timeout: 5 * time.Second, Transport: c.client.Transport, CheckRedirect: c.client.CheckRedirect}
	} else {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	resp, err := client.Do(req)
	if err != nil {
		return errors.New("request streaming provider manifest failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("streaming provider manifest returned status %d", resp.StatusCode)
	}
	var manifest stremioManifest
	if err := decodeBoundedJSON(resp.Body, maxManifestResponseBytes, &manifest); err != nil {
		return fmt.Errorf("decode streaming provider manifest: %w", err)
	}
	return validateStremioManifest(manifest)
}

func validateStremioManifest(manifest stremioManifest) error {
	if strings.TrimSpace(manifest.ID) == "" {
		return errors.New("streaming provider manifest is missing id")
	}
	hasStreamResource := false
	for _, raw := range manifest.Resources {
		var name string
		if err := json.Unmarshal(raw, &name); err != nil {
			var descriptor struct {
				Name string `json:"name"`
			}
			if err := json.Unmarshal(raw, &descriptor); err == nil {
				name = descriptor.Name
			}
		}
		if strings.EqualFold(strings.TrimSpace(name), "stream") {
			hasStreamResource = true
			break
		}
	}
	if !hasStreamResource {
		return errors.New("streaming provider manifest does not advertise the stream resource")
	}
	for _, mediaType := range manifest.Types {
		if strings.EqualFold(strings.TrimSpace(mediaType), "movie") || strings.EqualFold(strings.TrimSpace(mediaType), "series") {
			return nil
		}
	}
	return errors.New("streaming provider manifest does not advertise movie or series support")
}

func decodeBoundedJSON(body io.Reader, limit int64, destination any) error {
	data, err := io.ReadAll(io.LimitReader(body, limit+1))
	if err != nil {
		return err
	}
	if int64(len(data)) > limit {
		return fmt.Errorf("response exceeds %d bytes", limit)
	}
	return json.Unmarshal(data, destination)
}

func newProviderHTTPClient() *http.Client {
	return newRestrictedRedirectHTTPClient(45 * time.Second)
}

// sameParentDomain reports whether host a and host b share at least two
// rightmost domain labels (e.g. "v3-cinemeta.strem.io" and
// "cinemeta-live.strem.io" both end with ".strem.io").
func sameParentDomain(a, b string) bool {
	aParts := strings.Split(strings.TrimSuffix(a, "."), ".")
	bParts := strings.Split(strings.TrimSuffix(b, "."), ".")
	if len(aParts) < 3 || len(bParts) < 3 {
		return false
	}
	aLast := strings.ToLower(strings.Join(aParts[len(aParts)-2:], "."))
	bLast := strings.ToLower(strings.Join(bParts[len(bParts)-2:], "."))
	return aLast == bLast
}

func newRestrictedRedirectHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		CheckRedirect: func(request *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return errors.New("too many redirects")
			}
			if len(via) == 0 {
				return nil
			}
			origin := via[0].URL
			target := request.URL
			if target.User != nil {
				return errors.New("redirect with userinfo is not allowed")
			}
			if !strings.EqualFold(origin.Scheme, target.Scheme) {
				return errors.New("cross-scheme redirects are not allowed")
			}
			if strings.EqualFold(origin.Host, target.Host) {
				return nil
			}
			// Allow same-registered-domain redirects so well-known
			// metadata providers redirect within their own domain.
			if sameParentDomain(origin.Host, target.Host) {
				return nil
			}
			return errors.New("cross-origin redirects are not allowed")
		},
	}
}

func (c *manifestStreamResolver) normalizeSeriesProviderID(ctx context.Context, mediaID string) (string, error) {
	parts := strings.Split(mediaID, ":")
	if len(parts) < 2 || !strings.EqualFold(parts[0], "tvdb") {
		return mediaID, nil
	}
	tvdbID := strings.TrimSpace(parts[1])
	if tvdbID == "" {
		return "", errors.New("TVDB series ID is empty")
	}
	lookupURL := tvmazeBaseURL + "/lookup/shows?thetvdb=" + url.QueryEscape(tvdbID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, lookupURL, nil)
	if err != nil {
		return "", fmt.Errorf("create TVDB series lookup: %w", err)
	}
	client := c.client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("lookup TVDB series ID %s: %w", tvdbID, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("TVMaze returned status %d for TVDB series ID %s", resp.StatusCode, tvdbID)
	}
	var payload struct {
		Externals struct {
			IMDb string `json:"imdb"`
		} `json:"externals"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&payload); err != nil {
		return "", fmt.Errorf("decode TVDB series lookup: %w", err)
	}
	imdbID := strings.TrimSpace(payload.Externals.IMDb)
	if imdbID == "" {
		return "", fmt.Errorf("TVDB series ID %s has no IMDb ID for Stremio playback", tvdbID)
	}
	if len(parts) == 2 {
		return imdbID, nil
	}
	return imdbID + ":" + strings.Join(parts[2:], ":"), nil
}

func (c *manifestStreamResolver) GetVariants(ctx context.Context, virtualPath string) []runtimehost.VirtualMediaVariant {
	return c.GetConfiguredVariants(virtualPath)
}

func profileByLabel(profiles []QualityProfile, label string) QualityProfile {
	for _, profile := range profiles {
		if strings.EqualFold(profile.Label, label) {
			return profile
		}
	}
	return QualityProfile{}
}

func (c *manifestStreamResolver) qualityConfig() QualityConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.config.Quality
}

func candidateVariantID(candidate StreamCandidate) string {
	// Provider URLs are temporary and commonly rotate credentials/query tokens.
	// Hash only stable, provider-visible fields so result= handles survive a
	// refresh and provider reordering. Providers that expose a stable video hash
	// contribute it to the identity; otherwise stable display and URL-path fields
	// distinguish candidates.
	urlIdentity := ""
	if parsed, err := url.Parse(strings.TrimSpace(candidate.URL)); err == nil {
		filename := strings.TrimSpace(candidate.BehaviorHints.Filename)
		if filename == "" {
			filename = path.Base(parsed.Path)
		}
		urlIdentity = strings.ToLower(parsed.Scheme) + "://" + strings.ToLower(parsed.Host) + "/" + strings.ToLower(filename)
	}
	fingerprint := strings.Join([]string{
		strings.TrimSpace(candidate.Name), strings.TrimSpace(candidate.Title),
		strconv.FormatInt(candidate.FileSize, 10),
		strings.TrimSpace(candidate.Resolution), strings.TrimSpace(candidate.CodecVideo),
		strings.TrimSpace(candidate.CodecAudio), strings.TrimSpace(candidate.HDR),
		strings.TrimSpace(candidate.SourceType), strings.TrimSpace(candidate.Container),
		strings.Join(candidate.AudioLanguages, ","), strings.Join(candidate.SubtitleLanguages, ","),
		strings.TrimSpace(candidate.BehaviorHints.VideoHash),
		strings.TrimSpace(candidate.BehaviorHints.Filename),
		strings.TrimSpace(candidate.BehaviorHints.BingeGroup),
		urlIdentity,
	}, "\x00")
	digest := sha256.Sum256([]byte(fingerprint))
	return hex.EncodeToString(digest[:12])
}

func candidateDisplayName(candidate StreamCandidate) string {
	name := strings.TrimSpace(candidate.Name)
	if name == "" {
		name = strings.TrimSpace(candidate.Title)
	}
	if name == "" {
		name = candidate.Resolution
	}
	if size := streamSize(candidate); size != "" && !strings.Contains(strings.ToLower(name), strings.ToLower(size)) {
		name += " · " + size
	}
	clean := strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			return -1
		}
		return r
	}, name)
	return strings.TrimSpace(clean)
}

// GetConfiguredVariants exposes profile-specific virtual URIs without querying
// the upstream provider. This keeps request fulfillment instant while allowing
// Silo to display every configured quality choice; the profile is resolved only
// when the URI is played.
func (c *manifestStreamResolver) GetConfiguredVariants(virtualPath string) []runtimehost.VirtualMediaVariant {
	c.mu.RLock()
	config := c.config.Quality
	c.mu.RUnlock()
	if !config.EnableProfiles {
		// The canonical URI is already persisted on the item. The provider
		// response carries the complete ranked candidate list for failover.
		return nil
	}
	variants := make([]runtimehost.VirtualMediaVariant, 0, len(config.Profiles))
	for _, profile := range config.Profiles {
		if strings.TrimSpace(profile.Label) == "" {
			continue
		}
		values := url.Values{}
		values.Set("profile", profile.Label)
		variants = append(variants, runtimehost.VirtualMediaVariant{
			VirtualURI: virtualPath + "?" + values.Encode(),
			Label:      profile.Label,
			Resolution: profile.Resolution,
			CodecVideo: profile.CodecVideo,
			CodecAudio: profile.CodecAudio,
			HDR:        profile.HDR,
		})
	}
	return variants
}

func parseVirtualPath(virtualPath string) (string, string, error) {
	if !strings.HasPrefix(virtualPath, virtualPathPrefix) {
		return "", "", errors.New("path is not an virtual URI")
	}
	cleanPath := virtualPath
	if idx := strings.Index(cleanPath, "?"); idx != -1 {
		cleanPath = cleanPath[:idx]
	}
	parts := strings.Split(strings.Trim(strings.TrimPrefix(cleanPath, virtualPathPrefix), "/"), "/")
	if len(parts) < 2 {
		return "", "", errors.New("virtual URI must contain a media type and identifier")
	}
	mediaType := strings.ToLower(parts[0])
	if mediaType != "movie" && mediaType != "series" && mediaType != "anime" {
		return "", "", fmt.Errorf("unsupported virtual media type %q", mediaType)
	}
	mediaID := strings.Join(parts[1:], ":")
	if strings.ContainsAny(mediaID, "?#") || strings.Contains(mediaID, "..") {
		return "", "", errors.New("virtual URI contains an invalid identifier")
	}
	return mediaType, mediaID, nil
}

func streamEndpoint(manifestURL, mediaType, mediaID string) (string, error) {
	return streamEndpointWithPolicy(manifestURL, mediaType, mediaID, false)
}

func streamEndpointWithPolicy(manifestURL, mediaType, mediaID string, allowInsecure bool) (string, error) {
	manifest, err := url.Parse(strings.TrimSpace(manifestURL))
	if err != nil || manifest.Host == "" || (manifest.Scheme != "https" && manifest.Scheme != "http") || (manifest.Scheme != "https" && !allowInsecure) {
		return "", errors.New("a valid streaming provider manifest URL is required (HTTPS, or HTTP with Allow local HTTP enabled for private/local hosts)")
	}
	if manifest.Scheme == "http" && !isPrivateHost(manifest.Hostname()) {
		return "", errors.New("insecure HTTP is allowed only for private/local streaming provider hosts")
	}
	if !strings.HasSuffix(manifest.Path, "/manifest.json") {
		return "", errors.New("streaming provider URL must end in /manifest.json")
	}
	manifest.Path = strings.TrimSuffix(manifest.Path, "/manifest.json") + "/stream/" + url.PathEscape(mediaType) + "/" + url.PathEscape(mediaID) + ".json"
	manifest.RawQuery = ""
	manifest.Fragment = ""
	return manifest.String(), nil
}

func isPrivateHost(host string) bool {
	host = strings.TrimSpace(strings.ToLower(host))
	// Single-label names are normally Docker/Kubernetes service names (for
	// example "virtual" or "altmount") and are not public DNS names.
	if host == "localhost" || strings.HasSuffix(host, ".local") || !strings.Contains(host, ".") {
		return true
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() {
		return true
	}
	return false
}

type runtimeServer struct {
	runtimedefault.Server
	pb.UnimplementedRequestRouterServer
	pb.UnimplementedScheduledTaskServer
	configMu     sync.Mutex
	manifest     *pb.PluginManifest
	resolver     *manifestStreamResolver
	monitor      *mediaMonitor
	library      virtualMediaRegistrar
	releaseStore *release.ReleaseStore
	scheduler    *release.Scheduler
}

func (s *runtimeServer) GetManifest(context.Context, *pb.GetManifestRequest) (*pb.GetManifestResponse, error) {
	return &pb.GetManifestResponse{Manifest: s.manifest}, nil
}
func (s *runtimeServer) Configure(_ context.Context, request *pb.ConfigureRequest) (*pb.ConfigureResponse, error) {
	s.configMu.Lock()
	defer s.configMu.Unlock()
	for _, entry := range request.GetConfig() {
		if entry.GetKey() != configKey {
			continue
		}
		values := entry.GetValue().AsMap()
		manifestURL, _ := values["manifest_url"].(string)
		tmdbAPIKey, _ := values["tmdb_api_key"].(string)
		allowInsecure, _ := values["allow_insecure_http"].(bool)
		cacheTTLMinutes, err := parseCacheTTLMinutes(values["cache_ttl_minutes"])
		if err != nil {
			return nil, err
		}
		if strings.TrimSpace(manifestURL) == "" {
			return &pb.ConfigureResponse{}, nil
		}
		if _, err := streamEndpointWithPolicy(manifestURL, "movie", "tt0000001", allowInsecure); err != nil {
			return nil, err
		}

		var qc QualityConfig
		qc.Preset, _ = values["quality_preset"].(string)
		qc.CustomFormatPreset, _ = values["custom_format_preset"].(string)
		qc.EnableProfiles, _ = values["enable_quality_profiles"].(bool)
		qc.FallbackToAnyStream, _ = values["fallback_to_any_stream"].(bool)
		if v, ok := values["single_stream_with_failover"].(bool); ok {
			qc.SingleStreamWithFailover = v
		} else {
			qc.SingleStreamWithFailover = true
		}
		if rawFormats, ok := values["custom_formats"]; ok {
			data, marshalErr := json.Marshal(rawFormats)
			if marshalErr != nil || json.Unmarshal(data, &qc.CustomFormats) != nil {
				return nil, errors.New("custom_formats: invalid configuration")
			}
		}
		profiles, err := decodeQualityProfiles(values["quality_profiles"])
		if err != nil {
			return nil, fmt.Errorf("quality_profiles: %w", err)
		}
		qc.Profiles = profiles
		qc.ApplyPreset()

		if err := qc.Validate(); err != nil {
			return nil, fmt.Errorf("invalid quality config: %w", err)
		}

		monitorFile, _ := entry.GetValue().AsMap()["monitor_file"].(string)
		monitorFile = resolvePluginDataPath(monitorFile, ".silo-virtual-library-monitored.json")
		prowlarrIndexFile, _ := entry.GetValue().AsMap()["prowlarr_index_file"].(string)
		prowlarrIndexFile = resolvePluginDataPath(prowlarrIndexFile, ".silo-virtual-library-prowlarr-index.json")
		movieLibraryID, err := configuredFolderID(entry.GetValue().AsMap()["movie_library_id"])
		if err != nil {
			return nil, err
		}
		seriesLibraryID, err := configuredFolderID(entry.GetValue().AsMap()["series_library_id"])
		if err != nil {
			return nil, err
		}
		// On a fresh server the host has no libraries yet.  The user must
		// create at least one Movie and one Series library before this plugin
		// can register virtual media into them.  Log a warning but don't
		// prevent startup when a configured library ID no longer exists --
		// the admin UI needs to load so the user can fix the IDs.
		if movieLibraryID > 0 || seriesLibraryID > 0 {
			if err := validateLibraryIDs(sdkruntime.Host(), movieLibraryID, seriesLibraryID); err != nil {
				hclog.New(&hclog.LoggerOptions{Name: "silo-virtual-library"}).Warn("library configuration needs attention", "error", err)
			}
		}
		stagedMonitorConfig, monitoredItems, err := loadMonitorConfig(monitorConfig{TMDBAPIKey: strings.TrimSpace(tmdbAPIKey), File: strings.TrimSpace(monitorFile), ProwlarrIndexFile: strings.TrimSpace(prowlarrIndexFile), FilterProwlarr: qc.EnableProfiles, Quality: qc})
		if err != nil {
			return nil, err
		}
		if s.scheduler != nil {
			s.scheduler.SetCatalogPath(monitorFile)
			if minutes, ok := entry.GetValue().AsMap()["schedule_refresh_minutes"].(float64); ok && minutes > 0 {
				s.scheduler.SetInterval(time.Duration(minutes) * time.Minute)
			}
		}
		library, err := newSiloLibrary(sdkruntime.Host(), movieLibraryID, seriesLibraryID, s.resolver)
		if err != nil {
			return nil, err
		}
		// Every fallible validation step completes before any live component is
		// changed, so a rejected configuration cannot leave mixed old/new state.
		rssURL, _ := values["indexer_rss_url"].(string)
		rssKey, _ := values["indexer_api_key"].(string)
		rssMinutes, _ := values["indexer_rss_check_minutes"].(float64)
		s.resolver.Configure(resolverConfig{
			ManifestURL:            manifestURL,
			AllowInsecure:          allowInsecure,
			Quality:                qc,
			CacheTTLMinutes:        cacheTTLMinutes,
			TMDBAPIKey:             strings.TrimSpace(tmdbAPIKey),
			IndexerRSSURL:          strings.TrimSpace(rssURL),
			IndexerAPIKey:          strings.TrimSpace(rssKey),
			IndexerRSSCheckMinutes: int(rssMinutes),
		})
		s.library = library
		// Support multiple RSS feed URLs separated by newlines so users
		// can add every Prowlarr indexer. One shared key and interval.
		if err := s.monitor.configureProwlarr(strings.TrimSpace(rssURL), strings.TrimSpace(rssKey), int(rssMinutes), stagedMonitorConfig.ProwlarrIndexFile); err != nil {
			return nil, err
		}
		s.monitor.applyConfiguration(stagedMonitorConfig, monitoredItems, library, true)
		return &pb.ConfigureResponse{}, nil
	}
	// No streaming config yet — accept empty configure so the plugin
	// starts and can serve dynamic config options (library dropdowns).
	return &pb.ConfigureResponse{}, nil
}

func main() {
	manifest, err := loadManifest()
	if err != nil {
		fmt.Fprintf(os.Stderr, "load manifest: %v\n", err)
		os.Exit(1)
	}
	releaseStore := release.NewReleaseStore()
	releaseClient := release.NewMetadataClient(metadataClient)
	scheduler := release.NewScheduler(releaseStore, releaseClient, 6*time.Hour)

	resolver := &manifestStreamResolver{
		client:       newProviderHTTPClient(),
		releaseStore: releaseStore,
		logger:       hclog.New(&hclog.LoggerOptions{Name: "silo-virtual-library-resolver"}),
	}
	monitor := newMediaMonitor(resolver, hclog.New(&hclog.LoggerOptions{Name: "silo-virtual-library-monitor"}))
	monitor.releaseStore = releaseStore

	scheduler.SetShowProvider(func(ctx context.Context) ([]string, error) {
		monitor.mu.Lock()
		defer monitor.mu.Unlock()
		var ids []string
		for _, item := range monitor.items {
			if item.MediaType == "series" && item.IMDbID != "" {
				ids = append(ids, item.IMDbID)
			}
		}
		return ids, nil
	})

	go scheduler.Start(context.Background(), resolvePluginDataPath(".silo-virtual-library-monitored.json", ".silo-virtual-library-monitored.json"))

	runtime := &runtimeServer{
		manifest:     manifest,
		resolver:     resolver,
		monitor:      monitor,
		releaseStore: releaseStore,
		scheduler:    scheduler,
	}
	sdkruntime.Serve(sdkruntime.ServeConfig{
		Logger: hclog.New(&hclog.LoggerOptions{Name: "silo-virtual-library"}),
		Servers: sdkruntime.CapabilityServers{
			Runtime:               runtime,
			VirtualStreamProvider: &virtualStreamProvider{resolver: resolver},
			RequestRouter:         runtime,
			ScheduledTask:         runtime,
			HttpRoutes:            newAdminRoutes(runtime),
		},
	})
}
func loadManifest() (*pb.PluginManifest, error) {
	manifest, err := publicmanifest.Load(manifestJSON)
	if err != nil {
		return nil, err
	}
	if manifest.Checksum == "" {
		executable, err := os.Executable()
		if err == nil {
			if binary, err := os.ReadFile(executable); err == nil {
				checksum := sha256.Sum256(binary)
				manifest.Checksum = hex.EncodeToString(checksum[:])
			}
		}
	}
	return manifest, nil
}
