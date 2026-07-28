package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	pb "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"github.com/Silo-Server/silo-plugin-sdk/pkg/pluginsdk/runtimehost"
)

func TestManifestStreamResolverResolvesFirstHTTPStream(t *testing.T) {
	var gotPath string
	client := &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		gotPath = r.URL.Path
		body, _ := json.Marshal(map[string]any{"streams": []map[string]string{
			{"url": "magnet:?xt=urn:btih:ignored"},
			{"url": "https://stream.example/movie.mkv?token=secret"},
		}})
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(body)), Header: make(http.Header)}, nil
	})}

	resolver := &manifestStreamResolver{client: client}
	resolver.Configure(resolverConfig{ManifestURL: "https://aio.example/configured/manifest.json"})
	streamURL, err := resolver.Resolve(context.Background(), "virtual://movie/tt0133093")
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
		{"virtual://movie/tt0133093", "movie", "tt0133093", false},
		{"virtual://series/tt0944947/1/2", "series", "tt0944947:1:2", false},
		{"virtual://anime/kitsu/12/1", "anime", "kitsu:12:1", false},
		{"/movie/tt0133093", "", "", true},
		{"virtual://book/1", "", "", true},
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
	if _, err := streamEndpoint("http://streaming:8080/token/manifest.json", "movie", "tt0133093"); err == nil {
		t.Fatal("streamEndpoint accepted HTTP without explicit opt-in")
	}
	endpoint, err := streamEndpointWithPolicy("http://streaming:8080/token/manifest.json", "movie", "tt0133093", true)
	if err != nil {
		t.Fatalf("private HTTP endpoint rejected: %v", err)
	}
	if endpoint != "http://streaming:8080/token/stream/movie/tt0133093.json" {
		t.Fatalf("endpoint = %q", endpoint)
	}
	if _, err := streamEndpointWithPolicy("http://altmount:8080/token/manifest.json", "movie", "tt0133093", true); err != nil {
		t.Fatalf("single-label service endpoint rejected: %v", err)
	}
	if _, err := streamEndpointWithPolicy("http://public.example/token/manifest.json", "movie", "tt0133093", true); err == nil {
		t.Fatal("HTTP public endpoint accepted")
	}
}

func TestPlaybackServerReturnsResolverFailureAsBadGateway(t *testing.T) {
	server := playbackServer{resolver: resolverFunc(func(context.Context, string) (string, error) {
		return "", context.DeadlineExceeded
	})}
	response, err := server.Handle(context.Background(), &pb.HandleHTTPRequest{Path: "virtual://movie/tt0133093"})
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
func (f resolverFunc) GetVariants(ctx context.Context, path string) []runtimehost.VirtualMediaVariant {
	return nil
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestQualityProfiles(t *testing.T) {
	client := &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		body, _ := json.Marshal(map[string]any{"streams": []map[string]string{
			{"url": "https://stream.example/movie1.mkv", "title": "720p HDTV"},
			{"url": "https://stream.example/movie2.mkv", "title": "1080p WEB-DL"},
			{"url": "https://stream.example/movie3.mkv", "title": "2160p REMUX Dolby Vision", "name": "4K Stream"},
			{"url": "https://stream.example/movie4.mkv", "title": "2160p HDR10 WEB-DL"},
		}})
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(body)), Header: make(http.Header)}, nil
	})}

	resolver := &manifestStreamResolver{client: client}

	qc := QualityConfig{
		EnableProfiles: true,
		Profiles: []QualityProfile{
			{Label: "4K DV", Resolution: "2160p", HDR: "dv"},
			{Label: "1080p", Resolution: "1080p"},
			{Label: "Exclude Remux", ExcludeRegex: "(?i)remux"},
			{Label: "Include HDR10", IncludeRegex: "(?i)hdr10"},
		},
		FallbackToAnyStream: true,
		MaxVersionsPerItem:  4,
	}
	qc.Validate()

	resolver.Configure(resolverConfig{ManifestURL: "https://aio.example/manifest.json", Quality: qc})

	ctx := context.Background()

	url1, err := resolver.Resolve(ctx, "virtual://movie/tt0133093?profile=4K+DV")
	if err != nil || url1 != "https://stream.example/movie3.mkv" {
		t.Fatalf("Expected movie3.mkv, got %v %v", url1, err)
	}

	url2, err := resolver.Resolve(ctx, "virtual://movie/tt0133093?profile=1080p")
	if err != nil || url2 != "https://stream.example/movie2.mkv" {
		t.Fatalf("Expected movie2.mkv, got %v %v", url2, err)
	}

	url3, err := resolver.Resolve(ctx, "virtual://movie/tt0133093?profile=Exclude+Remux")
	if err != nil || url3 != "https://stream.example/movie4.mkv" {
		t.Fatalf("Expected movie4.mkv (best without remux), got %v %v", url3, err)
	}

	url4, err := resolver.Resolve(ctx, "virtual://movie/tt0133093?profile=Include+HDR10")
	if err != nil || url4 != "https://stream.example/movie4.mkv" {
		t.Fatalf("Expected movie4.mkv, got %v %v", url4, err)
	}

	url5, err := resolver.Resolve(ctx, "virtual://movie/tt0133093")
	if err != nil || url5 != "https://stream.example/movie1.mkv" {
		t.Fatalf("Expected movie1.mkv (fallback any stream), got %v %v", url5, err)
	}

	variants := resolver.GetVariants(ctx, "virtual://movie/tt0133093")
	if len(variants) != 4 {
		t.Fatalf("Expected 4 profile placeholders, got %d", len(variants))
	}
	if strings.Contains(variants[0].VirtualURI, "result=") {
		t.Fatalf("Expected provider-neutral variant URI: %s", variants[0].VirtualURI)
	}
	selected, err := resolver.Resolve(ctx, variants[0].VirtualURI)
	if err != nil || selected != "https://stream.example/movie3.mkv" {
		t.Fatalf("Expected variant to resolve movie3.mkv, got %v %v", selected, err)
	}

	qc.FallbackToAnyStream = false
	resolver.Configure(resolverConfig{ManifestURL: "https://aio.example/manifest.json", Quality: qc})
	_, err = resolver.Resolve(ctx, "virtual://movie/tt0133093?profile=NonExistent")
	if err == nil {
		t.Fatalf("Expected error for non-existent profile when fallback is false")
	}

	qc.EnableProfiles = false
	resolver.Configure(resolverConfig{ManifestURL: "https://aio.example/manifest.json", Quality: qc})
	url6, err := resolver.Resolve(ctx, "virtual://movie/tt0133093?profile=4K+DV")
	if err != nil || url6 != "https://stream.example/movie1.mkv" {
		t.Fatalf("Expected movie1.mkv when profiles are disabled, got %v %v", url6, err)
	}
}

func TestQualityConfigValidation(t *testing.T) {
	qc := QualityConfig{
		EnableProfiles: true,
		Profiles: []QualityProfile{
			{Label: "P1", IncludeRegex: "["},
		},
	}
	err := qc.Validate()
	if err == nil {
		t.Fatalf("Expected error for invalid regex")
	}

	qc2 := QualityConfig{
		EnableProfiles: true,
		Profiles: []QualityProfile{
			{Label: "P1"}, {Label: "p1"},
		},
	}
	err2 := qc2.Validate()
	if err2 == nil {
		t.Fatalf("Expected error for duplicate label")
	}
}

func TestQualityConfigSelectionModeDefaultsAndValidates(t *testing.T) {
	qc := QualityConfig{}
	if err := qc.Validate(); err != nil {
		t.Fatalf("default selection mode rejected: %v", err)
	}
	if qc.SelectionMode != SelectionModeMaxVersions {
		t.Fatalf("default selection mode = %q, want %q", qc.SelectionMode, SelectionModeMaxVersions)
	}
	qc.SelectionMode = SelectionModePicker
	if err := qc.Validate(); err != nil {
		t.Fatalf("picker selection mode rejected: %v", err)
	}
	qc.SelectionMode = "unknown"
	if err := qc.Validate(); err == nil {
		t.Fatal("unknown selection mode accepted")
	}
}

func TestPickerSelectionModeDoesNotMaterializeVariants(t *testing.T) {
	resolver := &manifestStreamResolver{}
	resolver.Configure(resolverConfig{Quality: QualityConfig{
		EnableProfiles: true,
		SelectionMode:  SelectionModePicker,
		Profiles:       []QualityProfile{{Label: "1080p"}},
	}})
	if got := resolver.GetConfiguredVariants("virtual://movie/tt0133093"); len(got) != 0 {
		t.Fatalf("configured variants in picker mode = %d, want 0", len(got))
	}
}

func TestDecodeQualityProfilesAcceptsJSONStringsAndArrays(t *testing.T) {
	encoded := `[{"label":"1080p","resolution":"1080p","preferred_order":2}]`
	for name, raw := range map[string]any{
		"json string": encoded,
		"typed array": []any{map[string]any{"label": "1080p", "resolution": "1080p", "preferred_order": float64(2)}},
	} {
		t.Run(name, func(t *testing.T) {
			profiles, err := decodeQualityProfiles(raw)
			if err != nil {
				t.Fatalf("decodeQualityProfiles() error = %v", err)
			}
			if len(profiles) != 1 || profiles[0].Label != "1080p" || profiles[0].PreferredOrder != 2 {
				t.Fatalf("profiles = %#v", profiles)
			}
		})
	}
}

func TestCandidateDisplayNameIncludesProviderSize(t *testing.T) {
	name := candidateDisplayName(StreamCandidate{Name: "AltMount FHD", Title: "Movie 1080p WEB-DL 5.69 GB"})
	if !strings.Contains(name, "5.69 GB") {
		t.Fatalf("candidate display name = %q, want provider size", name)
	}
}

func TestParseStreamDetailsPrefersTitleResolutionOverURLTokens(t *testing.T) {
	candidate := StreamCandidate{
		Title: "Sin City A Dame to Kill For [720p]",
		URL:   "https://provider.example/play/4k opaque-token",
	}
	parseStreamDetails(&candidate)
	if candidate.Resolution != "720p" {
		t.Fatalf("resolution = %q, want 720p", candidate.Resolution)
	}
}
