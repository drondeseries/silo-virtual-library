package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	pb "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
)

func TestAIOStreamsClientResolvesFirstHTTPStream(t *testing.T) {
	var gotPath string
	client := &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		gotPath = r.URL.Path
		body, _ := json.Marshal(map[string]any{"streams": []map[string]string{
			{"url": "magnet:?xt=urn:btih:ignored"},
			{"url": "https://stream.example/movie.mkv?token=secret"},
		}})
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(body)), Header: make(http.Header)}, nil
	})}

	resolver := &aioStreamsClient{client: client}
	resolver.Configure(resolverConfig{ManifestURL: "https://aio.example/configured/manifest.json"})
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

func TestStreamEndpointAllowsPrivateHTTPOnlyWhenOptedIn(t *testing.T) {
	if _, err := streamEndpoint("http://aiostreams:8080/token/manifest.json", "movie", "tt0133093"); err == nil {
		t.Fatal("streamEndpoint accepted HTTP without explicit opt-in")
	}
	endpoint, err := streamEndpointWithPolicy("http://aiostreams:8080/token/manifest.json", "movie", "tt0133093", true)
	if err != nil {
		t.Fatalf("private HTTP endpoint rejected: %v", err)
	}
	if endpoint != "http://aiostreams:8080/token/stream/movie/tt0133093.json" {
		t.Fatalf("endpoint = %q", endpoint)
	}
	if _, err := streamEndpointWithPolicy("http://public.example/token/manifest.json", "movie", "tt0133093", true); err == nil {
		t.Fatal("HTTP public endpoint accepted")
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

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
