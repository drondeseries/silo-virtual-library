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
}
type resolverConfig struct{ ManifestURL string }
type aioStreamsClient struct {
	client *http.Client
	mu     sync.RWMutex
	config resolverConfig
}

type stremioResponse struct {
	Streams []stremioStream `json:"streams"`
}
type stremioStream struct {
	URL string `json:"url"`
}

func (c *aioStreamsClient) Configure(config resolverConfig) {
	c.mu.Lock()
	c.config = config
	c.mu.Unlock()
}

func (c *aioStreamsClient) Resolve(ctx context.Context, virtualPath string) (string, error) {
	mediaType, mediaID, err := parseVirtualPath(virtualPath)
	if err != nil {
		return "", err
	}
	c.mu.RLock()
	manifestURL := c.config.ManifestURL
	c.mu.RUnlock()
	endpoint, err := streamEndpoint(manifestURL, mediaType, mediaID)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", fmt.Errorf("create AIOStreams request: %w", err)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("request AIOStreams: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("AIOStreams returned status %d", resp.StatusCode)
	}
	var payload stremioResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(&payload); err != nil {
		return "", fmt.Errorf("decode AIOStreams response: %w", err)
	}
	for _, stream := range payload.Streams {
		candidate, parseErr := url.Parse(strings.TrimSpace(stream.URL))
		if parseErr == nil && candidate.IsAbs() && (candidate.Scheme == "https" || candidate.Scheme == "http") {
			return candidate.String(), nil
		}
	}
	return "", errors.New("AIOStreams returned no playable HTTP streams")
}

func parseVirtualPath(virtualPath string) (string, string, error) {
	if !strings.HasPrefix(virtualPath, virtualPathPrefix) {
		return "", "", errors.New("path is not an aiostreams URI")
	}
	parts := strings.Split(strings.Trim(strings.TrimPrefix(virtualPath, virtualPathPrefix), "/"), "/")
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
	manifest, err := url.Parse(strings.TrimSpace(manifestURL))
	if err != nil || manifest.Scheme != "https" || manifest.Host == "" {
		return "", errors.New("a valid HTTPS AIOStreams manifest URL is required")
	}
	if !strings.HasSuffix(manifest.Path, "/manifest.json") {
		return "", errors.New("AIOStreams URL must end in /manifest.json")
	}
	manifest.Path = strings.TrimSuffix(manifest.Path, "/manifest.json") + "/stream/" + url.PathEscape(mediaType) + "/" + url.PathEscape(mediaID) + ".json"
	manifest.RawQuery = ""
	manifest.Fragment = ""
	return manifest.String(), nil
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
		manifestURL, _ := entry.GetValue().AsMap()["manifest_url"].(string)
		if _, err := streamEndpoint(manifestURL, "movie", "tt0000001"); err != nil {
			return nil, err
		}
		s.resolver.Configure(resolverConfig{ManifestURL: manifestURL})
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
		library, err := newSiloLibrary(sdkruntime.Host(), movieLibraryID, seriesLibraryID)
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
