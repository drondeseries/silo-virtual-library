package main

import (
	"context"
	"encoding/json"
	"net/url"
	"testing"
	"time"

	pb "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
)

func TestPlaybackServerResolvesVirtualPath(t *testing.T) {
	fixedTime := time.Unix(1_700_000_000, 0)
	server := playbackServer{resolver: mockAIOStreamsResolver{
		baseURL: "https://resolver.example/stream",
		now:     func() time.Time { return fixedTime },
	}}

	response, err := server.Handle(context.Background(), &pb.HandleHTTPRequest{Path: "aiostreams://tmdb/movie/550"})
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if response.GetStatusCode() != 200 {
		t.Fatalf("Handle() status = %d, want 200", response.GetStatusCode())
	}

	var payload struct {
		StreamURL string `json:"stream_url"`
	}
	if err := json.Unmarshal(response.GetBody(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	parsed, err := url.Parse(payload.StreamURL)
	if err != nil {
		t.Fatalf("parse stream URL: %v", err)
	}
	if parsed.Path != "/stream/tmdb/movie/550/master.m3u8" {
		t.Errorf("stream path = %q", parsed.Path)
	}
	if parsed.Query().Get("expires") != "1700000900" {
		t.Errorf("expires = %q", parsed.Query().Get("expires"))
	}
	if parsed.Query().Get("token") == "" {
		t.Error("stream token is empty")
	}
}

func TestPlaybackServerIgnoresRegularPath(t *testing.T) {
	server := playbackServer{}
	response, err := server.Handle(context.Background(), &pb.HandleHTTPRequest{Path: "/movies/550"})
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if response.GetStatusCode() != 404 {
		t.Fatalf("Handle() status = %d, want 404", response.GetStatusCode())
	}
}
