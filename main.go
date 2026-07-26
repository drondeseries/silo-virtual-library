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
	"strings"
	"sync"
	"time"

	pb "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	publicmanifest "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginsdk/manifest"
	sdkruntime "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginsdk/runtime"
	"github.com/Silo-Server/silo-plugin-sdk/pkg/pluginsdk/runtimedefault"
	"github.com/Silo-Server/silo-plugin-sdk/pkg/pluginsdk/runtimehost"
	"github.com/hashicorp/go-hclog"
)

const (
	virtualPathPrefix = "virtual://"
	configKey         = "streaming"
	maxResponseBytes  = 4 << 20
)

//go:embed manifest.json
var manifestJSON []byte

type streamResolver interface {
	Resolve(context.Context, string) (string, error)
	GetVariants(context.Context, string) []runtimehost.VirtualMediaVariant
}
type resolverConfig struct {
	ManifestURL   string
	AllowInsecure bool
	Quality       QualityConfig
}
type manifestStreamResolver struct {
	client *http.Client
	mu     sync.RWMutex
	config resolverConfig
}

type stremioResponse struct {
	Streams []StreamCandidate `json:"streams"`
}

func (c *manifestStreamResolver) Configure(config resolverConfig) {
	c.mu.Lock()
	c.config = config
	c.mu.Unlock()
}

func (c *manifestStreamResolver) Resolve(ctx context.Context, virtualPath string) (string, error) {
	candidates, _, _, err := c.GetCandidates(ctx, virtualPath)
	if err != nil {
		return "", err
	}
	if len(candidates) == 0 {
		return "", errors.New("streaming provider returned no streams")
	}

	u, _ := url.Parse(virtualPath)
	requestedProfile := ""
	if u != nil {
		requestedProfile = u.Query().Get("profile")
	}

	c.mu.RLock()
	config := c.config.Quality
	c.mu.RUnlock()

	if !config.EnableProfiles || requestedProfile == "" {
		if requestedResult := u.Query().Get("result"); requestedResult != "" {
			for _, candidate := range candidates {
				if candidateVariantID(candidate) == requestedResult {
					return candidate.URL, nil
				}
			}
		}
		return candidates[0].URL, nil
	}
	if requestedResult := u.Query().Get("result"); requestedResult != "" {
		for _, candidate := range candidates {
			if candidateVariantID(candidate) == requestedResult && matchProfile(candidate, profileByLabel(config.Profiles, requestedProfile)) {
				return candidate.URL, nil
			}
		}
	}

	var matchProfileObj QualityProfile
	found := false
	for _, p := range config.Profiles {
		if strings.EqualFold(p.Label, requestedProfile) {
			matchProfileObj = p
			found = true
			break
		}
	}

	if found {
		var matched []StreamCandidate
		for _, cand := range candidates {
			if matchProfile(cand, matchProfileObj) {
				matched = append(matched, cand)
			}
		}
		if len(matched) > 0 {
			sortCandidatesForProfile(matched, matchProfileObj)
			return matched[0].URL, nil
		}
	}

	if config.FallbackToAnyStream {
		return candidates[0].URL, nil
	}

	return "", fmt.Errorf("no stream matches profile %q", requestedProfile)
}

func (c *manifestStreamResolver) GetCandidates(ctx context.Context, virtualPath string) ([]StreamCandidate, string, string, error) {
	mediaType, mediaID, err := parseVirtualPath(virtualPath)
	if err != nil {
		return nil, mediaType, mediaID, err
	}

	// strip query from mediaID
	if idx := strings.Index(mediaID, "?"); idx != -1 {
		mediaID = mediaID[:idx]
	}

	c.mu.RLock()
	manifestURL := c.config.ManifestURL
	allowInsecure := c.config.AllowInsecure
	c.mu.RUnlock()
	endpoint, err := streamEndpointWithPolicy(manifestURL, mediaType, mediaID, allowInsecure)
	if err != nil {
		return nil, mediaType, mediaID, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, mediaType, mediaID, fmt.Errorf("create streaming provider request: %w", err)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, mediaType, mediaID, fmt.Errorf("request streaming provider: %w", err)
	}
	defer resp.Body.Close()
	var validCandidates []StreamCandidate
	if resp.StatusCode != http.StatusOK {
		return validCandidates, mediaType, mediaID, fmt.Errorf("streaming provider returned status %d", resp.StatusCode)
	}
	var payload stremioResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(&payload); err != nil {
		return validCandidates, mediaType, mediaID, fmt.Errorf("decode streaming provider response: %w", err)
	}
	for i, stream := range payload.Streams {
		candidate, parseErr := url.Parse(strings.TrimSpace(stream.URL))
		if parseErr == nil && candidate.IsAbs() && (candidate.Scheme == "https" || candidate.Scheme == "http") {
			stream.OriginalIndex = i
			parseStreamDetails(&stream)
			validCandidates = append(validCandidates, stream)
		}
	}
	return validCandidates, mediaType, mediaID, nil
}

func (c *manifestStreamResolver) GetVariants(ctx context.Context, virtualPath string) []runtimehost.VirtualMediaVariant {
	var variants []runtimehost.VirtualMediaVariant
	c.mu.RLock()
	config := c.config.Quality
	c.mu.RUnlock()

	if !config.EnableProfiles {
		return variants
	}
	candidates, _, _, err := c.GetCandidates(ctx, virtualPath)
	if err != nil || len(candidates) == 0 {
		return variants
	}
	maxVersions := config.MaxVersionsPerItem
	if maxVersions <= 0 {
		maxVersions = len(candidates)
	}

	for _, p := range config.Profiles {
		var matched []StreamCandidate
		for _, cand := range candidates {
			if matchProfile(cand, p) {
				matched = append(matched, cand)
			}
		}
		if len(matched) > 0 {
			sortCandidatesForProfile(matched, p)
			profileSeen := make(map[string]struct{})
			for _, candidate := range matched {
				id := candidateVariantID(candidate)
				if _, ok := profileSeen[id]; ok {
					continue
				}
				profileSeen[id] = struct{}{}
				values := url.Values{}
				values.Set("profile", p.Label)
				values.Set("result", id)
				label := strings.TrimSpace(p.Label + " · " + candidateDisplayName(candidate))
				variants = append(variants, runtimehost.VirtualMediaVariant{VirtualURI: virtualPath + "?" + values.Encode(), Label: label, Resolution: candidate.Resolution, CodecVideo: candidate.CodecVideo, CodecAudio: candidate.CodecAudio, HDR: candidate.HDR})
				if len(profileSeen) >= maxVersions {
					break
				}
			}
		}
	}
	return variants
}

func profileByLabel(profiles []QualityProfile, label string) QualityProfile {
	for _, profile := range profiles {
		if strings.EqualFold(profile.Label, label) {
			return profile
		}
	}
	return QualityProfile{}
}

func candidateVariantID(candidate StreamCandidate) string {
	digest := sha256.Sum256([]byte(candidate.URL))
	return hex.EncodeToString(digest[:6])
}

func candidateDisplayName(candidate StreamCandidate) string {
	name := strings.TrimSpace(candidate.Name)
	if name == "" {
		name = strings.TrimSpace(candidate.Title)
	}
	if name == "" {
		name = candidate.Resolution
	}
	return name
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
		return nil
	}
	max := config.MaxVersionsPerItem
	if max <= 0 {
		max = len(config.Profiles)
	}
	variants := make([]runtimehost.VirtualMediaVariant, 0, minInt(max, len(config.Profiles)))
	for _, profile := range config.Profiles {
		if strings.TrimSpace(profile.Label) == "" {
			continue
		}
		variantURI := virtualPath + "?profile=" + url.QueryEscape(profile.Label)
		variants = append(variants, runtimehost.VirtualMediaVariant{
			VirtualURI: variantURI,
			Label:      profile.Label,
			Resolution: profile.Resolution,
			CodecVideo: profile.CodecVideo,
			CodecAudio: profile.CodecAudio,
			HDR:        profile.HDR,
		})
		if len(variants) >= max {
			break
		}
	}
	return variants
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
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
		return "", errors.New("a valid HTTPS streaming provider manifest URL is required")
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
	manifest *pb.PluginManifest
	resolver *manifestStreamResolver
	monitor  *mediaMonitor
	library  virtualMediaRegistrar
}

func (s *runtimeServer) GetManifest(context.Context, *pb.GetManifestRequest) (*pb.GetManifestResponse, error) {
	return &pb.GetManifestResponse{Manifest: s.manifest}, nil
}
func (s *runtimeServer) Configure(_ context.Context, request *pb.ConfigureRequest) (*pb.ConfigureResponse, error) {
	for _, entry := range request.GetConfig() {
		if entry.GetKey() != configKey {
			continue
		}
		values := entry.GetValue().AsMap()
		manifestURL, _ := values["manifest_url"].(string)
		allowInsecure, _ := values["allow_insecure_http"].(bool)
		if _, err := streamEndpointWithPolicy(manifestURL, "movie", "tt0000001", allowInsecure); err != nil {
			return nil, err
		}

		var qc QualityConfig
		qc.EnableProfiles, _ = values["enable_quality_profiles"].(bool)
		qc.FallbackToAnyStream, _ = values["fallback_to_any_stream"].(bool)

		if maxV, ok := values["max_versions_per_item"].(float64); ok {
			qc.MaxVersionsPerItem = int(maxV)
		} else {
			qc.MaxVersionsPerItem = 3
		}

		profiles, err := decodeQualityProfiles(values["quality_profiles"])
		if err != nil {
			return nil, fmt.Errorf("quality_profiles: %w", err)
		}
		qc.Profiles = profiles

		if err := qc.Validate(); err != nil {
			return nil, fmt.Errorf("invalid quality config: %w", err)
		}

		s.resolver.Configure(resolverConfig{ManifestURL: manifestURL, AllowInsecure: allowInsecure, Quality: qc})
		tmdbAPIKey, _ := entry.GetValue().AsMap()["tmdb_api_key"].(string)
		monitorFile, _ := entry.GetValue().AsMap()["monitor_file"].(string)
		movieLibraryID, err := configuredFolderID(entry.GetValue().AsMap()["movie_library_id"])
		if err != nil {
			return nil, err
		}
		seriesLibraryID, err := configuredFolderID(entry.GetValue().AsMap()["series_library_id"])
		if err != nil {
			return nil, err
		}
		library, err := newSiloLibrary(sdkruntime.Host(), movieLibraryID, seriesLibraryID, s.resolver)
		if err != nil {
			return nil, err
		}
		s.library = library
		s.monitor.setRegistrar(library)
		s.monitor.Configure(monitorConfig{TMDBAPIKey: strings.TrimSpace(tmdbAPIKey), File: strings.TrimSpace(monitorFile)})
		return &pb.ConfigureResponse{}, nil
	}
	return nil, fmt.Errorf("required %q configuration is missing", configKey)
}

type playbackServer struct {
	pb.UnimplementedHttpRoutesServer
	resolver streamResolver
}

func (s *playbackServer) Handle(ctx context.Context, request *pb.HandleHTTPRequest) (*pb.HandleHTTPResponse, error) {
	if request == nil {
		return jsonResponse(http.StatusNotFound, map[string]string{"error": "path is not handled by virtual playback"})
	}
	path := request.GetPath()
	if strings.HasPrefix(path, "/resolve/") {
		path = strings.TrimPrefix(path, "/resolve/")
	}
	if !strings.HasPrefix(path, virtualPathPrefix) {
		return jsonResponse(http.StatusNotFound, map[string]string{"error": "path is not handled by virtual playback"})
	}
	streamURL, err := s.resolver.Resolve(ctx, path)
	if err != nil {
		return jsonResponse(http.StatusBadGateway, map[string]string{"error": err.Error()})
	}
	return jsonResponse(http.StatusOK, map[string]string{"stream_url": streamURL})
}
func jsonResponse(status int, payload any) (*pb.HandleHTTPResponse, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return &pb.HandleHTTPResponse{StatusCode: int32(status), Headers: map[string]string{"content-type": "application/json"}, Body: body}, nil
}

func main() {
	manifest, err := loadManifest()
	if err != nil {
		fmt.Fprintf(os.Stderr, "load manifest: %v\n", err)
		os.Exit(1)
	}
	resolver := &manifestStreamResolver{client: &http.Client{Timeout: 45 * time.Second}}
	monitor := newMediaMonitor(resolver, hclog.New(&hclog.LoggerOptions{Name: "silo-virtual-library-monitor"}))
	runtime := &runtimeServer{manifest: manifest, resolver: resolver, monitor: monitor}
	sdkruntime.Serve(sdkruntime.ServeConfig{
		Logger:  hclog.New(&hclog.LoggerOptions{Name: "silo-virtual-library"}),
		Servers: sdkruntime.CapabilityServers{Runtime: runtime, HttpRoutes: &playbackServer{resolver: resolver}, RequestRouter: runtime, ScheduledTask: runtime},
	})
}
func loadManifest() (*pb.PluginManifest, error) {
	manifest, err := publicmanifest.Load(manifestJSON)
	if err != nil {
		return nil, err
	}
	executable, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("locate executable: %w", err)
	}
	binary, err := os.ReadFile(executable)
	if err != nil {
		return nil, fmt.Errorf("read executable: %w", err)
	}
	checksum := sha256.Sum256(binary)
	manifest.Checksum = hex.EncodeToString(checksum[:])
	return manifest, nil
}
