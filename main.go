package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/hashicorp/go-hclog"

	pb "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	publicmanifest "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginsdk/manifest"
	sdkruntime "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginsdk/runtime"
	"github.com/Silo-Server/silo-plugin-sdk/pkg/pluginsdk/runtimedefault"
)

const virtualPathPrefix = "aiostreams://"

//go:embed manifest.json
var manifestJSON []byte

// aioStreamsResolver is the seam where a real AIOStreams API client can be
// plugged in. The starter implementation deliberately performs no I/O.
type aioStreamsResolver interface {
	Resolve(context.Context, string) (string, error)
}

type mockAIOStreamsResolver struct {
	baseURL string
	now     func() time.Time
}

func (r mockAIOStreamsResolver) Resolve(_ context.Context, virtualPath string) (string, error) {
	mediaID := strings.TrimPrefix(virtualPath, virtualPathPrefix)
	mediaID = strings.TrimSpace(mediaID)
	if mediaID == "" {
		return "", errors.New("aiostreams path has no media identifier")
	}

	tokenBytes := make([]byte, 12)
	if _, err := rand.Read(tokenBytes); err != nil {
		return "", fmt.Errorf("generate mock stream token: %w", err)
	}

	streamURL, err := url.Parse(r.baseURL)
	if err != nil {
		return "", fmt.Errorf("parse resolver base URL: %w", err)
	}
	streamURL.Path = strings.TrimSuffix(streamURL.Path, "/") + "/" + mediaID + "/master.m3u8"
	query := streamURL.Query()
	query.Set("expires", fmt.Sprint(r.now().Add(15*time.Minute).Unix()))
	query.Set("token", hex.EncodeToString(tokenBytes))
	streamURL.RawQuery = query.Encode()

	return streamURL.String(), nil
}

type runtimeServer struct {
	runtimedefault.Server
	manifest *pb.PluginManifest
}

func (s *runtimeServer) GetManifest(context.Context, *pb.GetManifestRequest) (*pb.GetManifestResponse, error) {
	return &pb.GetManifestResponse{Manifest: s.manifest}, nil
}

// playbackServer handles the SDK's current gRPC request interception service.
// Non-virtual paths are rejected so they remain the responsibility of Silo.
type playbackServer struct {
	pb.UnimplementedHttpRoutesServer
	resolver aioStreamsResolver
}

func (s *playbackServer) Handle(ctx context.Context, request *pb.HandleHTTPRequest) (*pb.HandleHTTPResponse, error) {
	if request == nil || !strings.HasPrefix(request.GetPath(), virtualPathPrefix) {
		return &pb.HandleHTTPResponse{
			StatusCode: 404,
			Headers:    map[string]string{"content-type": "application/json"},
			Body:       []byte(`{"error":"path is not handled by virtual playback"}`),
		}, nil
	}

	streamURL, err := s.resolver.Resolve(ctx, request.GetPath())
	if err != nil {
		return nil, fmt.Errorf("resolve virtual playback path: %w", err)
	}

	body, err := json.Marshal(map[string]string{"stream_url": streamURL})
	if err != nil {
		return nil, fmt.Errorf("encode playback response: %w", err)
	}

	return &pb.HandleHTTPResponse{
		StatusCode: 200,
		Headers:    map[string]string{"content-type": "application/json"},
		Body:       body,
	}, nil
}

func main() {
	manifest, err := loadManifest()
	if err != nil {
		fmt.Fprintf(os.Stderr, "load manifest: %v\n", err)
		os.Exit(1)
	}

	resolver := mockAIOStreamsResolver{
		baseURL: "https://resolver.aiostreams.example/stream",
		now:     time.Now,
	}

	sdkruntime.Serve(sdkruntime.ServeConfig{
		Logger: hclog.New(&hclog.LoggerOptions{Name: "silo-virtual-library"}),
		Servers: sdkruntime.CapabilityServers{
			Runtime:    &runtimeServer{manifest: manifest},
			HttpRoutes: &playbackServer{resolver: resolver},
		},
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
