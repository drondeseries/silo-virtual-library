package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	pb "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
)

func TestAIOStreamsClientResolvesFirstHTTPStream(t *testing.T) {
	var gotPath string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		_ = json.NewEncoder(w).Encode(map[string]any{"streams": []map[string]string{
			{"url": "magnet:?xt=urn:btih:ignored"},
			{"url": "https://stream.example/movie.mkv?token=secret"},
		}})
	}))
	defer server.Close()

	resolver := &aioStreamsClient{client: server.Client()}
	resolver.Configure(resolverConfig{ManifestURL: server.URL + "/configured/manifest.json"})
	streamURL, err := resolver.Resolve(context.Background(), "aiostreams://movie/tt0133093")
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if streamURL != "https://stream.example/movie.mkv?token=secret" {
		t.Fatalf("Resolve() = %q", streamURL)
	}
	if gotPath != "/configured/stream/movie/tt0133093.json" {
		t.Fatalf("request path = %q", gotPath)
	}
}

func TestParseVirtualPath(t *testing.T) {
	tests := []struct {
		path, mediaType, mediaID string
		wantErr                  bool
	}{
		{"aiostreams://movie/tt0133093", "movie", "tt0133093", false},
		{"aiostreams://series/tt0944947/1/2", "series", "tt0944947:1:2", false},
		{"aiostreams://anime/kitsu/12/1", "anime", "kitsu:12:1", false},
		{"/movie/tt0133093", "", "", true},
		{"aiostreams://book/1", "", "", true},
	}
	for _, tc := range tests {
		t.Run(tc.path, func(t *testing.T) {
			mediaType, mediaID, err := parseVirtualPath(tc.path)
			if (err != nil) != tc.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tc.wantErr)
			}
			if mediaType != tc.mediaType || mediaID != tc.mediaID {
				t.Fatalf("got (%q, %q), want (%q, %q)", mediaType, mediaID, tc.mediaType, tc.mediaID)
			}
		})
	}
}

func TestPlaybackServerReturnsResolverFailureAsBadGateway(t *testing.T) {
	server := playbackServer{resolver: resolverFunc(func(context.Context, string) (string, error) {
		return "", context.DeadlineExceeded
	})}
	response, err := server.Handle(context.Background(), &pb.HandleHTTPRequest{Path: "aiostreams://movie/tt0133093"})
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if response.GetStatusCode() != http.StatusBadGateway {
		t.Fatalf("Handle() status = %d", response.GetStatusCode())
	}
}

func TestPlaybackServerIgnoresRegularPath(t *testing.T) {
	server := playbackServer{}
	response, err := server.Handle(context.Background(), &pb.HandleHTTPRequest{Path: "/movies/550"})
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if response.GetStatusCode() != http.StatusNotFound {
		t.Fatalf("Handle() status = %d", response.GetStatusCode())
	}
}

type resolverFunc func(context.Context, string) (string, error)

func (f resolverFunc) Resolve(ctx context.Context, path string) (string, error) { return f(ctx, path) }
