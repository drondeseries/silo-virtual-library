package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
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

func TestResolveVirtualStreamSingleStreamWithFailoverResultsAll(t *testing.T) {
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
	metadata, err := structpb.NewStruct(map[string]any{
		"virtual_uri": "virtual://movie/tt1234567?results=all",
	})
	if err != nil {
		t.Fatalf("NewStruct: %v", err)
	}
	resp, err := provider.ResolveVirtualStream(context.Background(), &pb.ResolveVirtualStreamRequest{
		MediaType:   "movie",
		ExternalIds: map[string]string{"imdb": "tt1234567"},
		Metadata:    metadata,
	})
	if err != nil {
		t.Fatalf("ResolveVirtualStream() error = %v", err)
	}
	if resp.GetResult() == nil || len(resp.GetResult().GetCandidates()) != 3 {
		t.Fatalf("candidates count = %d, want 3 candidates", len(resp.GetResult().GetCandidates()))
	}
	candidates := resp.GetResult().GetCandidates()
	for i, cand := range candidates {
		v, ok := cand.GetMetadata().GetFields()["visible"].GetKind().(*structpb.Value_BoolValue)
		if !ok || !v.BoolValue {
			t.Fatalf("candidate %d visible = %v, want true when results=all", i, v)
		}
	}
}

// A forced refresh with no excluded candidates is a genuine re-list: the host
// is recovering from a dead stream (relay 502) and must get a fresh provider
// answer even when the cache entry is younger than freshServeFloor.
func TestResolveVirtualStreamForceRefreshRelistsWithinFloor(t *testing.T) {
	var calls atomic.Int32
	client := &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		call := calls.Add(1)
		body, _ := json.Marshal(map[string]any{"streams": []map[string]string{
			{"title": "Stream", "url": fmt.Sprintf("https://provider.example/%d.mkv", call)},
		}})
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(body)), Header: make(http.Header)}, nil
	})}
	resolver := &manifestStreamResolver{client: client}
	resolver.Configure(resolverConfig{ManifestURL: "https://provider.example/manifest.json"})
	provider := &virtualStreamProvider{resolver: resolver}

	first, err := provider.ResolveVirtualStream(context.Background(), &pb.ResolveVirtualStreamRequest{
		MediaType:   "movie",
		ExternalIds: map[string]string{"imdb": "tt1234567"},
	})
	if err != nil {
		t.Fatalf("first ResolveVirtualStream() error = %v", err)
	}
	if len(first.GetResult().GetCandidates()) != 1 {
		t.Fatalf("first candidates = %d, want 1", len(first.GetResult().GetCandidates()))
	}

	// The forced re-list must hit the provider again even though the first
	// answer is younger than freshServeFloor.
	metadata, err := structpb.NewStruct(map[string]any{"force_refresh": true})
	if err != nil {
		t.Fatalf("NewStruct: %v", err)
	}
	refreshed, err := provider.ResolveVirtualStream(context.Background(), &pb.ResolveVirtualStreamRequest{
		MediaType:   "movie",
		ExternalIds: map[string]string{"imdb": "tt1234567"},
		Metadata:    metadata,
	})
	if err != nil {
		t.Fatalf("refreshed ResolveVirtualStream() error = %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("provider calls = %d, want 2 (re-list must bypass the fresh floor)", calls.Load())
	}
	if len(refreshed.GetResult().GetCandidates()) != 1 ||
		refreshed.GetResult().GetCandidates()[0].GetTemporaryUri() == first.GetResult().GetCandidates()[0].GetTemporaryUri() {
		t.Fatalf("refreshed candidate = %q, want a new provider URL", refreshed.GetResult().GetCandidates()[0].GetTemporaryUri())
	}
}

// A forced refresh WITH excluded candidates is a failover walk inside one
// playback start: the freshServeFloor still applies so the walk stays on a
// single provider round-trip.
func TestResolveVirtualStreamForceRefreshWithExclusionsKeepsFloor(t *testing.T) {
	var calls atomic.Int32
	client := &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		call := calls.Add(1)
		body, _ := json.Marshal(map[string]any{"streams": []map[string]string{
			{"title": "Stream", "url": fmt.Sprintf("https://provider.example/%d.mkv", call)},
		}})
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(body)), Header: make(http.Header)}, nil
	})}
	resolver := &manifestStreamResolver{client: client}
	resolver.Configure(resolverConfig{ManifestURL: "https://provider.example/manifest.json"})
	provider := &virtualStreamProvider{resolver: resolver}

	if _, err := provider.ResolveVirtualStream(context.Background(), &pb.ResolveVirtualStreamRequest{
		MediaType:   "movie",
		ExternalIds: map[string]string{"imdb": "tt1234567"},
	}); err != nil {
		t.Fatalf("first ResolveVirtualStream() error = %v", err)
	}

	metadata, err := structpb.NewStruct(map[string]any{"force_refresh": true})
	if err != nil {
		t.Fatalf("NewStruct: %v", err)
	}
	if _, err := provider.ResolveVirtualStream(context.Background(), &pb.ResolveVirtualStreamRequest{
		MediaType:            "movie",
		ExternalIds:          map[string]string{"imdb": "tt1234567"},
		Metadata:             metadata,
		ExcludedCandidateIds: []string{"dead-candidate"},
	}); err != nil {
		t.Fatalf("failover ResolveVirtualStream() error = %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("provider calls = %d, want 1 (failover walk must keep the fresh floor)", calls.Load())
	}
}

func TestResolveVirtualStreamMarksAndRanksConfirmedCandidates(t *testing.T) {
	client := &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		body, _ := json.Marshal(map[string]any{"streams": []map[string]string{
			{"name": "Release B", "title": "2160p B", "url": "https://provider.example/b.mkv"},
			{"name": "Release A", "title": "1080p A", "url": "https://provider.example/a.mkv"},
		}})
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(body)), Header: make(http.Header)}, nil
	})}
	resolver := &manifestStreamResolver{client: client}
	resolver.Configure(resolverConfig{ManifestURL: "https://provider.example/manifest.json"})
	resolver.SetCandidateClassifier(mapClassifier{confirmed: map[string]bool{"Release A": true}})
	provider := &virtualStreamProvider{resolver: resolver}
	resp, err := provider.ResolveVirtualStream(context.Background(), &pb.ResolveVirtualStreamRequest{
		MediaType:   "movie",
		ExternalIds: map[string]string{"imdb": "tt1234567"},
	})
	if err != nil {
		t.Fatalf("ResolveVirtualStream() error = %v", err)
	}
	candidates := resp.GetResult().GetCandidates()
	if len(candidates) != 2 {
		t.Fatalf("candidates = %d, want 2", len(candidates))
	}
	// The completed release leads despite its lower resolution.
	confirmed, ok := candidates[0].GetMetadata().GetFields()["source_confirmed"].GetKind().(*structpb.Value_BoolValue)
	if !ok || !confirmed.BoolValue {
		t.Fatalf("first candidate metadata source_confirmed = %v, want true", confirmed)
	}
	if candidates[0].GetMetadata().GetFields()["display_name"].GetStringValue() == "" {
		t.Fatal("confirmed candidate should still carry a display name")
	}
}
