package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	pb "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestVirtualPathForRequestUsesProviderIDs(t *testing.T) {
	cases := []struct {
		name string
		req  *pb.ResolveVirtualStreamRequest
		want string
	}{
		{"movie imdb", &pb.ResolveVirtualStreamRequest{MediaType: "movie", ExternalIds: map[string]string{"imdb": "tt123"}}, "virtual://movie/tt123"},
		{"episode tvdb", &pb.ResolveVirtualStreamRequest{MediaType: "episode", ExternalIds: map[string]string{"tvdb": "456"}, SeasonNumber: 2, EpisodeNumber: 3}, "virtual://series/tvdb:456/2/3"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := virtualPathForRequest(tc.req)
			if err != nil || got != tc.want {
				t.Fatalf("virtualPathForRequest() = %q, %v; want %q", got, err, tc.want)
			}
		})
	}
}

func TestVirtualPathForRequestRequiresEpisodeCoordinates(t *testing.T) {
	_, err := virtualPathForRequest(&pb.ResolveVirtualStreamRequest{MediaType: "episode", ExternalIds: map[string]string{"imdb": "tt123"}})
	if err == nil {
		t.Fatal("expected missing season/episode error")
	}
}

func TestResolveVirtualStreamSingleStreamWithFailover(t *testing.T) {
	client := &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		body, _ := json.Marshal(map[string]any{"streams": []map[string]string{
			{"title": "2160p Stream 1", "url": "https://provider.example/4k1.mkv"},
			{"title": "2160p Stream 2", "url": "https://provider.example/4k2.mkv"},
			{"title": "1080p Stream 1", "url": "https://provider.example/1080.mkv"},
		}})
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(body)), Header: make(http.Header)}, nil
	})}
	resolver := &manifestStreamResolver{client: client}
	resolver.Configure(resolverConfig{
		ManifestURL: "https://provider.example/manifest.json",
		Quality:     QualityConfig{SingleStreamWithFailover: true},
	})
	provider := &virtualStreamProvider{resolver: resolver}
	resp, err := provider.ResolveVirtualStream(context.Background(), &pb.ResolveVirtualStreamRequest{
		MediaType:   "movie",
		ExternalIds: map[string]string{"imdb": "tt1234567"},
	})
	if err != nil {
		t.Fatalf("ResolveVirtualStream() error = %v", err)
	}
	if resp.GetResult() == nil || len(resp.GetResult().GetCandidates()) != 3 {
		t.Fatalf("candidates count = %d, want 3 candidates for failover", len(resp.GetResult().GetCandidates()))
	}
	candidates := resp.GetResult().GetCandidates()
	// Rank 1 should be visible
	v0, ok0 := candidates[0].GetMetadata().GetFields()["visible"].GetKind().(*structpb.Value_BoolValue)
	if !ok0 || !v0.BoolValue {
		t.Fatalf("candidate 0 visible = %v, want true", v0)
	}
	// Rank 2 and 3 should be hidden from catalog UI but retained for failover
	v1, ok1 := candidates[1].GetMetadata().GetFields()["visible"].GetKind().(*structpb.Value_BoolValue)
	if !ok1 || v1.BoolValue {
		t.Fatalf("candidate 1 visible = %v, want false", v1)
	}
	v2, ok2 := candidates[2].GetMetadata().GetFields()["visible"].GetKind().(*structpb.Value_BoolValue)
	if !ok2 || v2.BoolValue {
		t.Fatalf("candidate 2 visible = %v, want false", v2)
	}
}
