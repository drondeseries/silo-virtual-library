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
	virtualPathPrefix = "aiostreams://"
	configKey         = "aiostreams"
	maxResponseBytes  = 4 << 20
)

//go:embed manifest.json
var manifestJSON []byte

type aioStreamsResolver interface {
	Resolve(context.Context, string) (string, error)
	GetVariants(context.Context, string) []runtimehost.VirtualMediaVariant
}
type resolverConfig struct {
	ManifestURL   string
	AllowInsecure bool
	Quality       QualityConfig
}
type aioStreamsClient struct {
	client *http.Client
	mu     sync.RWMutex
	config resolverConfig
}

type stremioResponse struct {
	Streams []StreamCandidate `json:"streams"`
}

func (c *aioStreamsClient) Configure(config resolverConfig) {
	c.mu.Lock()
	c.config = config
	c.mu.Unlock()
}

func (c *aioStreamsClient) Resolve(ctx context.Context, virtualPath string) (string, error) {
	candidates, _, _, err := c.GetCandidates(ctx, virtualPath)
	if err != nil {
		return "", err
	}
	if len(candidates) == 0 {
		return "", errors.New("AIOStreams returned no streams")
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
		return candidates[0].URL, nil
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

func (c *aioStreamsClient) GetCandidates(ctx context.Context, virtualPath string) ([]StreamCandidate, string, string, error) {
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
		return nil, mediaType, mediaID, fmt.Errorf("create AIOStreams request: %w", err)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, mediaType, mediaID, fmt.Errorf("request AIOStreams: %w", err)
	}
	defer resp.Body.Close()
	var validCandidates []StreamCandidate
	if resp.StatusCode != http.StatusOK {
		return validCandidates, mediaType, mediaID, fmt.Errorf("AIOStreams returned status %d", resp.StatusCode)
	}
	var payload stremioResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(&payload); err != nil {
		return validCandidates, mediaType, mediaID, fmt.Errorf("decode AIOStreams response: %w", err)
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

func (c *aioStreamsClient) GetVariants(ctx context.Context, virtualPath string) []runtimehost.VirtualMediaVariant {
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

	for _, p := range config.Profiles {
		var matched []StreamCandidate
		for _, cand := range candidates {
			if matchProfile(cand, p) {
				matched = append(matched, cand)
			}
		}
		if len(matched) > 0 {
			sortCandidatesForProfile(matched, p)
			top := matched[0]
			variants = append(variants, runtimehost.VirtualMediaVariant{
				VirtualURI: virtualPath + "?profile=" + url.QueryEscape(p.Label),
				Label:      p.Label,
				Resolution: top.Resolution,
				CodecVideo: top.CodecVideo,
				CodecAudio: top.CodecAudio,
				HDR:        top.HDR,
			})
			if len(variants) >= config.MaxVersionsPerItem {
				break
			}
		}
	}
	return variants
}

func parseVirtualPath(virtualPath string) (string, string, error) {
	if !strings.HasPrefix(virtualPath, virtualPathPrefix) {
		return "", "", errors.New("path is not an aiostreams URI")
	}
	cleanPath := virtualPath
	if idx := strings.Index(cleanPath, "?"); idx != -1 {
		cleanPath = cleanPath[:idx]
	}
	parts := strings.Split(strings.Trim(strings.TrimPrefix(cleanPath, virtualPathPrefix), "/"), "/")
	if len(parts) < 2 {
		return "", "", errors.New("aiostreams URI must contain a media type and identifier")
	}
	mediaType := strings.ToLower(parts[0])
	if mediaType != "movie" && mediaType != "series" && mediaType != "anime" {
		return "", "", fmt.Errorf("unsupported aiostreams media type %q", mediaType)
	}
	mediaID := strings.Join(parts[1:], ":")
	if strings.ContainsAny(mediaID, "?#") || strings.Contains(mediaID, "..") {
		return "", "", errors.New("aiostreams URI contains an invalid identifier")
	}
	return mediaType, mediaID, nil
}

func streamEndpoint(manifestURL, mediaType, mediaID string) (string, error) {
	return streamEndpointWithPolicy(manifestURL, mediaType, mediaID, false)
}

func streamEndpointWithPolicy(manifestURL, mediaType, mediaID string, allowInsecure bool) (string, error) {
	manifest, err := url.Parse(strings.TrimSpace(manifestURL))
	if err != nil || manifest.Host == "" || (manifest.Scheme != "https" && manifest.Scheme != "http") || (manifest.Scheme != "https" && !allowInsecure) {
		return "", errors.New("a valid HTTPS AIOStreams manifest URL is required")
	}
	if manifest.Scheme == "http" && !isPrivateHost(manifest.Hostname()) {
		return "", errors.New("insecure HTTP is allowed only for private/local AIOStreams hosts")
	}
	if !strings.HasSuffix(manifest.Path, "/manifest.json") {
		return "", errors.New("AIOStreams URL must end in /manifest.json")
	}
	manifest.Path = strings.TrimSuffix(manifest.Path, "/manifest.json") + "/stream/" + url.PathEscape(mediaType) + "/" + url.PathEscape(mediaID) + ".json"
	manifest.RawQuery = ""
	manifest.Fragment = ""
	return manifest.String(), nil
}

func isPrivateHost(host string) bool {
	host = strings.TrimSpace(strings.ToLower(host))
	// Single-label names are normally Docker/Kubernetes service names (for
	// example "aiostreams" or "altmount") and are not public DNS names.
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
	resolver *aioStreamsClient
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

		if profilesRaw, ok := values["quality_profiles"].([]interface{}); ok {
			for _, pr := range profilesRaw {
				if pMap, ok := pr.(map[string]interface{}); ok {
					var p QualityProfile
					if v, ok := pMap["label"].(string); ok { p.Label = v }
					if v, ok := pMap["resolution"].(string); ok { p.Resolution = v }
					if v, ok := pMap["include_regex"].(string); ok { p.IncludeRegex = v }
					if v, ok := pMap["exclude_regex"].(string); ok { p.ExcludeRegex = v }
					if v, ok := pMap["preferred_order"].(float64); ok { p.PreferredOrder = int(v) }
					if v, ok := pMap["codec_video"].(string); ok { p.CodecVideo = v }
					if v, ok := pMap["codec_audio"].(string); ok { p.CodecAudio = v }
					if v, ok := pMap["hdr"].(string); ok { p.HDR = v }
					qc.Profiles = append(qc.Profiles, p)
				}
			}
		}

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
	resolver aioStreamsResolver
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
	resolver := &aioStreamsClient{client: &http.Client{Timeout: 45 * time.Second}}
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
