package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"github.com/hashicorp/go-hclog"
	"google.golang.org/protobuf/types/known/structpb"
)

var (
	tmdbBaseURL      = "https://api.themoviedb.org/3"
	cinemetaBaseURL  = "https://v3-cinemeta.strem.io"
	tvmazeBaseURL    = "https://api.tvmaze.com"
	errNoHomeRelease = errors.New("TMDB metadata has no digital or physical release date")
)

type monitorConfig struct{ TMDBAPIKey, File string }
type monitoredMedia struct {
	Key, MediaType, Title, StreamID, IMDbID, TMDBID, TVDBID string
	MediaFolderID                                           int       `json:"media_folder_id,omitempty"`
	Year                                                    int32     `json:"year"`
	Runtime                                                 int       `json:"runtime,omitempty"`
	Release                                                 time.Time `json:"release"`
	Ready                                                   bool      `json:"ready"`
	Overview, Poster, Backdrop                              string
	Genres                                                  []string
	Episodes                                                []virtualEpisode
}
type virtualEpisode struct {
	Season, Episode            int
	Runtime                    int
	Title, Overview, Thumbnail string
	Released                   time.Time
}
type mediaMonitor struct {
	mu        sync.Mutex
	resolver  aioStreamsResolver
	logger    hclog.Logger
	config    monitorConfig
	items     map[string]monitoredMedia
	registrar virtualMediaRegistrar
}

type virtualMediaLister interface {
	ListVirtual(context.Context) ([]monitoredMedia, error)
}

func (m *mediaMonitor) setRegistrar(registrar virtualMediaRegistrar) {
	m.mu.Lock()
	m.registrar = registrar
	m.mu.Unlock()
}

func (m *mediaMonitor) register(ctx context.Context, item monitoredMedia) error {
	m.mu.Lock()
	registrar := m.registrar
	m.mu.Unlock()
	if registrar == nil {
		return errors.New("Silo virtual catalog service is not configured")
	}
	return registrar.Register(ctx, item)
}

func (m *mediaMonitor) prewarm(path string) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = m.resolver.Resolve(ctx, path)
	}()
}

func newMediaMonitor(resolver aioStreamsResolver, logger hclog.Logger) *mediaMonitor {
	return &mediaMonitor{resolver: resolver, logger: logger, config: monitorConfig{File: ".silo-virtual-library-monitored.json"}, items: map[string]monitoredMedia{}}
}
func (m *mediaMonitor) Configure(c monitorConfig) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c.File == "" {
		c.File = ".silo-virtual-library-monitored.json"
	}
	m.config = c
	m.items = map[string]monitoredMedia{}
	data, err := os.ReadFile(c.File)
	if err == nil {
		var items []monitoredMedia
		if json.Unmarshal(data, &items) == nil {
			for _, item := range items {
				m.items[item.Key] = item
			}
		}
	}
}
func (m *mediaMonitor) remember(item monitoredMedia) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.items[item.Key] = item
	return m.saveLocked()
}
func (m *mediaMonitor) forget(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.items, key)
	return m.saveLocked()
}
func (m *mediaMonitor) item(key string) (monitoredMedia, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.items[key]
	return v, ok
}
func (m *mediaMonitor) saveLocked() error {
	items := make([]monitoredMedia, 0, len(m.items))
	for _, item := range m.items {
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Key < items[j].Key })
	data, err := json.MarshalIndent(items, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(m.config.File)
	tmp, err := os.CreateTemp(dir, ".silo-monitor-*.tmp")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if _, err = tmp.Write(append(data, '\n')); err == nil {
		err = tmp.Close()
	} else {
		_ = tmp.Close()
	}
	if err != nil {
		return err
	}
	return os.Rename(name, m.config.File)
}

func mediaFromRequest(r *pb.RequestDescriptor) (monitoredMedia, error) {
	if r == nil {
		return monitoredMedia{}, errors.New("request descriptor is required")
	}
	typ := strings.ToLower(strings.TrimSpace(r.GetMediaType()))
	if typ != "movie" && typ != "series" {
		return monitoredMedia{}, errors.New("media type must be movie or series")
	}
	ids := r.GetExternalIds()
	imdb, tmdb, tvdb := strings.TrimSpace(ids["imdb"]), strings.TrimSpace(ids["tmdb"]), strings.TrimSpace(ids["tvdb"])
	streamID := imdb
	if streamID == "" && tmdb != "" {
		streamID = "tmdb:" + tmdb
	}
	if streamID == "" {
		return monitoredMedia{}, errors.New("IMDb or TMDB ID is required")
	}
	return monitoredMedia{Key: typ + ":" + streamID, MediaType: typ, Title: strings.TrimSpace(r.GetTitle()), Year: r.GetYear(), StreamID: streamID, IMDbID: imdb, TMDBID: tmdb, TVDBID: tvdb}, nil
}

func (m *mediaMonitor) evaluate(ctx context.Context, item monitoredMedia) (monitoredMedia, string) {
	now := time.Now()
	if enriched, err := m.fetchCinemeta(ctx, item); err == nil && (item.MediaType != "series" || episodeMetadataComplete(enriched.Episodes)) {
		item = enriched
		if item.MediaType == "series" && episodeRuntimeMissing(item.Episodes) {
			if supplemented, supplementErr := m.fetchTVMaze(ctx, item); supplementErr == nil {
				item = supplemented
			}
		}
	} else {
		if item.MediaType == "series" {
			if fallback, fallbackErr := m.fetchTVMaze(ctx, item); fallbackErr == nil {
				item = fallback
			} else {
				m.logger.Warn("fetch series metadata", "key", item.Key, "cinemeta_error", err, "tvmaze_error", fallbackErr)
			}
		} else {
			m.logger.Warn("fetch Cinemeta metadata", "key", item.Key, "error", err)
		}
	}
	if item.MediaType == "movie" {
		if runtime, err := m.fetchTMDBMovieRuntime(ctx, item); err == nil && runtime > 0 {
			item.Runtime = runtime
		}
		release, err := m.movieRelease(ctx, item)
		if err != nil {
			item.Ready = false
			if errors.Is(err, errNoHomeRelease) {
				return item, "Movie is theatrical-only; waiting for a home-media release"
			}
			return item, "Release metadata unavailable; monitoring will retry"
		}
		item.Release = release
		if release.After(now) {
			item.Ready = false
			return item, "Movie is not released for home media yet"
		}
	}
	if item.MediaType == "series" {
		aired := make([]virtualEpisode, 0, len(item.Episodes))
		for _, episode := range item.Episodes {
			if episode.Season <= 0 || episode.Episode <= 0 || episode.Released.After(now) {
				continue
			}
			aired = append(aired, episode)
		}
		item.Episodes = aired
		item.Ready = len(aired) > 0
		if !item.Ready {
			return item, "No episodes have aired yet"
		}
		return item, fmt.Sprintf("%d aired episodes registered for on-demand playback", len(aired))
	}
	item.Ready = true
	return item, "Movie is available for home media"
}

func episodeMetadataComplete(episodes []virtualEpisode) bool {
	if len(episodes) == 0 {
		return false
	}
	for _, episode := range episodes {
		if episode.Season > 0 && episode.Episode > 0 && strings.TrimSpace(episode.Title) == "" {
			return false
		}
	}
	return true
}

func episodeRuntimeMissing(episodes []virtualEpisode) bool {
	for _, episode := range episodes {
		if episode.Season > 0 && episode.Episode > 0 && episode.Runtime <= 0 {
			return true
		}
	}
	return false
}

func (m *mediaMonitor) probeEpisodes(ctx context.Context, streamID string, episodes []virtualEpisode) []virtualEpisode {
	const concurrency = 4
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	playable := make([]virtualEpisode, 0, len(episodes))
	for _, episode := range episodes {
		episode := episode
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-sem }()
			path := fmt.Sprintf("aiostreams://series/%s/%d/%d", streamID, episode.Season, episode.Episode)
			if _, err := m.resolver.Resolve(ctx, path); err == nil {
				mu.Lock()
				playable = append(playable, episode)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	sort.Slice(playable, func(i, j int) bool {
		if playable[i].Season != playable[j].Season {
			return playable[i].Season < playable[j].Season
		}
		return playable[i].Episode < playable[j].Episode
	})
	return playable
}

func (s *runtimeServer) Fulfill(ctx context.Context, req *pb.FulfillRequest) (*pb.FulfillResponse, error) {
	item, err := mediaFromRequest(req.GetRequest())
	if err != nil {
		return nil, err
	}
	// Explicit request-time prewarming only. Scheduled monitoring never calls
	// this path, so theatrical titles are not repeatedly sent upstream.
	if item.MediaType == "movie" {
		s.monitor.prewarm("aiostreams://movie/" + strings.ReplaceAll(item.StreamID, ":", "/"))
	}
	item, message := s.monitor.evaluate(ctx, item)
	if item.MediaType == "series" && len(item.Episodes) > 0 {
		for _, episode := range item.Episodes {
			s.monitor.prewarm(fmt.Sprintf("aiostreams://series/%s/%d/%d", item.StreamID, episode.Season, episode.Episode))
		}
	}
	if len(req.GetConnections()) > 0 {
		folderID, folderErr := configuredFolderID(req.GetConnections()[0].GetConfig().AsMap()["media_folder_id"])
		if folderErr != nil {
			return nil, folderErr
		}
		item.MediaFolderID = folderID
	}
	if item.Ready {
		if err := s.monitor.register(ctx, item); err != nil {
			return nil, fmt.Errorf("register virtual media: %w", err)
		}
		message = "Virtual media registered in Silo library"
	}
	if !item.Ready || item.MediaType == "series" {
		if err := s.monitor.remember(item); err != nil {
			return nil, fmt.Errorf("persist monitored media: %w", err)
		}
	}
	status, external := "queued", "monitored"
	if item.Ready {
		status, external = "completed", "registered"
	}
	targets := make([]*pb.FulfillmentTarget, 0, len(req.GetQualities()))
	conn := ""
	if len(req.GetConnections()) > 0 {
		conn = req.GetConnections()[0].GetId()
	}
	for _, q := range req.GetQualities() {
		targets = append(targets, &pb.FulfillmentTarget{Quality: q.GetId(), ConnectionId: conn, ExternalId: item.Key, Status: status, ExternalStatus: external, Message: message})
	}
	return &pb.FulfillResponse{Targets: targets, Message: message}, nil
}

func (s *runtimeServer) CheckStatus(ctx context.Context, req *pb.CheckStatusRequest) (*pb.CheckStatusResponse, error) {
	base, err := mediaFromRequest(req.GetRequest())
	if err != nil {
		return nil, err
	}
	statuses := make([]*pb.TargetStatus, 0, len(req.GetTargets()))
	for _, target := range req.GetTargets() {
		item, ok := s.monitor.item(target.GetExternalId())
		if !ok {
			item = base
		}
		item, message := s.monitor.evaluate(ctx, item)
		if item.Ready {
			if err := s.monitor.register(ctx, item); err != nil {
				return nil, fmt.Errorf("register virtual media: %w", err)
			}
			message = "Virtual media registered in Silo library"
			if item.MediaType == "series" {
				_ = s.monitor.remember(item)
			} else {
				_ = s.monitor.forget(item.Key)
			}
		} else {
			_ = s.monitor.remember(item)
		}
		status, external := "queued", "monitored"
		if item.Ready {
			status, external = "completed", "registered"
		}
		statuses = append(statuses, &pb.TargetStatus{Quality: target.GetQuality(), ConnectionId: target.GetConnectionId(), Status: status, ExternalStatus: external, Message: message})
	}
	return &pb.CheckStatusResponse{Statuses: statuses}, nil
}
func (s *runtimeServer) ListConfigOptions(context.Context, *pb.ListConfigOptionsRequest) (*pb.ListConfigOptionsResponse, error) {
	return &pb.ListConfigOptionsResponse{OptionsByField: map[string]*pb.ConfigOptionList{}}, nil
}
func (s *runtimeServer) Validate(context.Context, *pb.ValidateRequest) (*pb.ValidateResponse, error) {
	return &pb.ValidateResponse{FieldErrors: map[string]string{}}, nil
}
func (s *runtimeServer) TestConnection(ctx context.Context, _ *pb.TestConnectionRequest) (*pb.TestConnectionResponse, error) {
	_, err := s.resolver.Resolve(ctx, "aiostreams://movie/tt0133093")
	if err != nil {
		return &pb.TestConnectionResponse{Ok: false, Message: err.Error()}, nil
	}
	return &pb.TestConnectionResponse{Ok: true, Message: "Connected to AIOStreams"}, nil
}
func (s *runtimeServer) Run(ctx context.Context, req *pb.RunScheduledTaskRequest) (*pb.RunScheduledTaskResponse, error) {
	if req.GetTaskKey() != "" && req.GetTaskKey() != "monitor-media" {
		return nil, fmt.Errorf("unknown task key %q", req.GetTaskKey())
	}
	s.monitor.mu.Lock()
	itemsByKey := make(map[string]monitoredMedia, len(s.monitor.items))
	for _, v := range s.monitor.items {
		itemsByKey[v.Key] = v
	}
	registrar := s.monitor.registrar
	s.monitor.mu.Unlock()
	if lister, ok := registrar.(virtualMediaLister); ok {
		existing, err := lister.ListVirtual(ctx)
		if err != nil {
			return nil, fmt.Errorf("list existing virtual media: %w", err)
		}
		for _, item := range existing {
			itemsByKey[item.Key] = item
		}
	}
	items := make([]monitoredMedia, 0, len(itemsByKey))
	for _, item := range itemsByKey {
		items = append(items, item)
	}
	ready, pending := 0, 0
	for _, item := range items {
		updated, _ := s.monitor.evaluate(ctx, item)
		if updated.Ready {
			if err := s.monitor.register(ctx, updated); err != nil {
				pending++
				s.monitor.logger.Error("register virtual media", "key", updated.Key, "error", err)
				continue
			}
			ready++
			if updated.MediaType == "series" {
				if err := s.monitor.remember(updated); err != nil {
					return nil, err
				}
			} else if err := s.monitor.forget(updated.Key); err != nil {
				return nil, err
			}
		} else {
			pending++
			if err := s.monitor.remember(updated); err != nil {
				return nil, err
			}
		}
	}
	out, _ := structpb.NewStruct(map[string]any{"media_checked": len(items), "ready": ready, "pending": pending})
	return &pb.RunScheduledTaskResponse{Output: out}, nil
}

func (m *mediaMonitor) fetchCinemeta(ctx context.Context, item monitoredMedia) (monitoredMedia, error) {
	if item.IMDbID == "" {
		return item, errors.New("IMDb ID required for Cinemeta metadata")
	}
	endpoint := strings.TrimRight(cinemetaBaseURL, "/") + "/meta/" + item.MediaType + "/" + url.PathEscape(item.IMDbID) + ".json"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return item, err
	}
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Do(req)
	if err != nil {
		return item, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return item, fmt.Errorf("Cinemeta HTTP %d", resp.StatusCode)
	}
	var payload struct {
		Meta struct {
			Name, Description, Poster, Background string
			Runtime                               string `json:"runtime"`
			Genres                                []string
			Videos                                []struct {
				ID, Title, Overview, Thumbnail string
				Season, Episode                int
				Released                       time.Time
			} `json:"videos"`
		} `json:"meta"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(&payload); err != nil {
		return item, err
	}
	if payload.Meta.Name != "" {
		item.Title = payload.Meta.Name
	}
	item.Overview, item.Poster, item.Backdrop, item.Genres = payload.Meta.Description, payload.Meta.Poster, payload.Meta.Background, payload.Meta.Genres
	if runtime := parseRuntimeMinutes(payload.Meta.Runtime); runtime > 0 {
		item.Runtime = runtime
	}
	item.Episodes = item.Episodes[:0]
	for _, video := range payload.Meta.Videos {
		item.Episodes = append(item.Episodes, virtualEpisode{Season: video.Season, Episode: video.Episode, Title: video.Title, Overview: video.Overview, Thumbnail: video.Thumbnail, Released: video.Released})
	}
	return item, nil
}

var htmlTagPattern = regexp.MustCompile(`<[^>]*>`)
var runtimeHourPattern = regexp.MustCompile(`(?i)(\d+)\s*h`)
var runtimeMinutePattern = regexp.MustCompile(`(?i)(\d+)\s*m`)
var runtimeNumberPattern = regexp.MustCompile(`\d+`)

func parseRuntimeMinutes(value string) int {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	hours, minutes := 0, 0
	if match := runtimeHourPattern.FindStringSubmatch(value); len(match) == 2 {
		hours, _ = strconv.Atoi(match[1])
	}
	if match := runtimeMinutePattern.FindStringSubmatch(value); len(match) == 2 {
		minutes, _ = strconv.Atoi(match[1])
	}
	if hours > 0 || minutes > 0 {
		return hours*60 + minutes
	}
	if match := runtimeNumberPattern.FindString(value); match != "" {
		minutes, _ = strconv.Atoi(match)
	}
	return minutes
}

func cleanTVMazeSummary(value string) string {
	return strings.TrimSpace(html.UnescapeString(htmlTagPattern.ReplaceAllString(value, "")))
}

func (m *mediaMonitor) fetchTVMaze(ctx context.Context, item monitoredMedia) (monitoredMedia, error) {
	if item.IMDbID == "" && item.TVDBID == "" && item.TMDBID != "" {
		m.mu.Lock()
		apiKey := m.config.TMDBAPIKey
		m.mu.Unlock()
		if apiKey != "" {
			if externalIDs, err := fetchTMDBExternalIDs(ctx, item.TMDBID, apiKey); err == nil {
				if externalIDs.IMDbID != "" {
					item.IMDbID = externalIDs.IMDbID
				}
				if externalIDs.TVDBID > 0 {
					item.TVDBID = strconv.Itoa(externalIDs.TVDBID)
				}
			}
		}
	}
	lookup, err := url.Parse(strings.TrimRight(tvmazeBaseURL, "/") + "/lookup/shows")
	if err != nil {
		return item, err
	}
	query := lookup.Query()
	if item.IMDbID != "" {
		query.Set("imdb", item.IMDbID)
	} else if item.TVDBID != "" {
		query.Set("thetvdb", item.TVDBID)
	} else {
		return item, errors.New("IMDb or TVDB ID required for TVMaze")
	}
	lookup.RawQuery = query.Encode()
	client := &http.Client{Timeout: 20 * time.Second}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, lookup.String(), nil)
	resp, err := client.Do(req)
	if err != nil {
		return item, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return item, fmt.Errorf("TVMaze lookup HTTP %d", resp.StatusCode)
	}
	var show struct {
		ID            int `json:"id"`
		Name, Summary string
		Genres        []string
		Premiered     string
		Image         struct{ Medium, Original string }
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(&show); err != nil {
		return item, err
	}
	if show.ID <= 0 {
		return item, errors.New("TVMaze returned no show ID")
	}
	episodesURL := fmt.Sprintf("%s/shows/%d/episodes", strings.TrimRight(tvmazeBaseURL, "/"), show.ID)
	episodeReq, _ := http.NewRequestWithContext(ctx, http.MethodGet, episodesURL, nil)
	episodeResp, err := client.Do(episodeReq)
	if err != nil {
		return item, err
	}
	defer episodeResp.Body.Close()
	if episodeResp.StatusCode != http.StatusOK {
		return item, fmt.Errorf("TVMaze episodes HTTP %d", episodeResp.StatusCode)
	}
	var episodes []struct {
		Name, Summary, Airdate, Airstamp string
		Season, Number                   int
		Runtime                          int
		Image                            struct{ Medium, Original string }
	}
	if err := json.NewDecoder(io.LimitReader(episodeResp.Body, maxResponseBytes)).Decode(&episodes); err != nil {
		return item, err
	}
	if show.Name != "" {
		item.Title = show.Name
	}
	item.Overview, item.Genres = cleanTVMazeSummary(show.Summary), show.Genres
	if show.Image.Original != "" {
		item.Poster = show.Image.Original
	} else {
		item.Poster = show.Image.Medium
	}
	item.Episodes = item.Episodes[:0]
	for _, episode := range episodes {
		released, parseErr := time.Parse(time.RFC3339, episode.Airstamp)
		if parseErr != nil && episode.Airdate != "" {
			released, _ = time.Parse("2006-01-02", episode.Airdate)
		}
		thumbnail := episode.Image.Original
		if thumbnail == "" {
			thumbnail = episode.Image.Medium
		}
		item.Episodes = append(item.Episodes, virtualEpisode{Season: episode.Season, Episode: episode.Number, Runtime: episode.Runtime, Title: episode.Name, Overview: cleanTVMazeSummary(episode.Summary), Thumbnail: thumbnail, Released: released})
	}
	return item, nil
}

type tmdbReleaseDates struct {
	Results []struct {
		Country string `json:"iso_3166_1"`
		Dates   []struct {
			Date time.Time `json:"release_date"`
			Type int       `json:"type"`
		} `json:"release_dates"`
	} `json:"results"`
}

func (m *mediaMonitor) movieRelease(ctx context.Context, item monitoredMedia) (time.Time, error) {
	m.mu.Lock()
	cfg := m.config
	m.mu.Unlock()
	if cfg.TMDBAPIKey != "" && item.TMDBID != "" {
		release, err := fetchTMDBRelease(ctx, item.TMDBID, cfg.TMDBAPIKey)
		if err == nil || errors.Is(err, errNoHomeRelease) {
			return release, err
		}
	}
	if item.IMDbID == "" {
		return time.Time{}, errors.New("IMDb ID required for Cinemeta fallback")
	}
	endpoint := strings.TrimRight(cinemetaBaseURL, "/") + "/meta/movie/" + url.PathEscape(item.IMDbID) + ".json"
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(request)
	if err != nil {
		return time.Time{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return time.Time{}, fmt.Errorf("Cinemeta HTTP %d", resp.StatusCode)
	}
	var payload struct {
		Meta struct {
			Released    time.Time `json:"released"`
			ReleaseInfo string    `json:"releaseInfo"`
			Year        string    `json:"year"`
		} `json:"meta"`
	}
	if err = json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(&payload); err != nil {
		return time.Time{}, err
	}
	if !payload.Meta.Released.IsZero() {
		// Cinemeta exposes the theatrical/premiere date, not a verified home
		// release. Do not let a newly opened theatrical title bypass the gate.
		// For older catalog titles, a conservative 90-day window keeps the
		// no-TMDB fallback useful without claiming day-one home availability.
		presumedHomeRelease := payload.Meta.Released.AddDate(0, 0, 90)
		if presumedHomeRelease.After(time.Now()) {
			return time.Time{}, errNoHomeRelease
		}
		return presumedHomeRelease, nil
	}
	for _, v := range []string{payload.Meta.ReleaseInfo, payload.Meta.Year} {
		if len(strings.TrimSpace(v)) >= 4 {
			if y, e := strconv.Atoi(strings.TrimSpace(v)[:4]); e == nil && y < time.Now().Year() {
				return time.Date(y, 1, 1, 0, 0, 0, 0, time.UTC), nil
			}
		}
	}
	return time.Time{}, errors.New("Cinemeta has no release date")
}

func (m *mediaMonitor) fetchTMDBMovieRuntime(ctx context.Context, item monitoredMedia) (int, error) {
	m.mu.Lock()
	key := m.config.TMDBAPIKey
	m.mu.Unlock()
	if key == "" || item.TMDBID == "" {
		return 0, nil
	}
	endpoint := strings.TrimRight(tmdbBaseURL, "/") + "/movie/" + url.PathEscape(item.TMDBID)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if strings.Count(key, ".") == 2 {
		req.Header.Set("Authorization", "Bearer "+key)
	} else {
		query := req.URL.Query()
		query.Set("api_key", key)
		req.URL.RawQuery = query.Encode()
	}
	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("TMDB movie details HTTP %d", resp.StatusCode)
	}
	var payload struct {
		Runtime int `json:"runtime"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(&payload); err != nil {
		return 0, err
	}
	return payload.Runtime, nil
}
func fetchTMDBRelease(ctx context.Context, id, key string) (time.Time, error) {
	endpoint := strings.TrimRight(tmdbBaseURL, "/") + "/movie/" + url.PathEscape(id) + "/release_dates"
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if strings.Count(key, ".") == 2 {
		req.Header.Set("Authorization", "Bearer "+key)
	} else {
		q := req.URL.Query()
		q.Set("api_key", key)
		req.URL.RawQuery = q.Encode()
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return time.Time{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return time.Time{}, fmt.Errorf("TMDB HTTP %d", resp.StatusCode)
	}
	var data tmdbReleaseDates
	if err = json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(&data); err != nil {
		return time.Time{}, err
	}
	results := data.Results
	for _, typ := range []int{4, 5} {
		var earliest time.Time
		for _, r := range results {
			for _, d := range r.Dates {
				if d.Type == typ && !d.Date.IsZero() && (earliest.IsZero() || d.Date.Before(earliest)) {
					earliest = d.Date
				}
			}
		}
		if !earliest.IsZero() {
			return earliest, nil
		}
	}
	return time.Time{}, errNoHomeRelease
}

type tmdbExternalIDs struct {
	IMDbID string `json:"imdb_id"`
	TVDBID int    `json:"tvdb_id"`
}

func fetchTMDBExternalIDs(ctx context.Context, tmdbID, key string) (tmdbExternalIDs, error) {
	endpoint := strings.TrimRight(tmdbBaseURL, "/") + "/tv/" + url.PathEscape(tmdbID) + "/external_ids"
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if strings.Count(key, ".") == 2 {
		req.Header.Set("Authorization", "Bearer "+key)
	} else {
		q := req.URL.Query()
		q.Set("api_key", key)
		req.URL.RawQuery = q.Encode()
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return tmdbExternalIDs{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return tmdbExternalIDs{}, fmt.Errorf("TMDB external_ids HTTP %d", resp.StatusCode)
	}
	var out tmdbExternalIDs
	if err = json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(&out); err != nil {
		return tmdbExternalIDs{}, err
	}
	return out, nil
}

