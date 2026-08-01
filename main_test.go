package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	publicmanifest "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginsdk/manifest"
	"github.com/Silo-Server/silo-plugin-sdk/pkg/pluginsdk/runtimehost"
	"google.golang.org/protobuf/types/known/structpb"
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

func TestGetCandidatesFreshBypassesCache(t *testing.T) {
	calls := 0
	client := &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		body, _ := json.Marshal(map[string]any{"streams": []map[string]string{{
			"url": fmt.Sprintf("https://stream.example/movie.mkv?token=%d", calls),
		}}})
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(body)), Header: make(http.Header)}, nil
	})}
	resolver := &manifestStreamResolver{client: client}
	resolver.Configure(resolverConfig{ManifestURL: "https://aio.example/manifest.json"})
	first, _, _, err := resolver.GetCandidates(context.Background(), "virtual://movie/tt0133093")
	if err != nil {
		t.Fatalf("GetCandidates() error = %v", err)
	}
	cached, _, _, err := resolver.GetCandidates(context.Background(), "virtual://movie/tt0133093")
	if err != nil {
		t.Fatalf("cached GetCandidates() error = %v", err)
	}
	fresh, _, _, err := resolver.GetCandidatesFresh(context.Background(), "virtual://movie/tt0133093")
	if err != nil {
		t.Fatalf("GetCandidatesFresh() error = %v", err)
	}
	if calls != 2 || first[0].URL != cached[0].URL || first[0].URL == fresh[0].URL {
		t.Fatalf("calls=%d first=%q cached=%q fresh=%q", calls, first[0].URL, cached[0].URL, fresh[0].URL)
	}
}

func TestConfigureDiscardsInFlightCandidateCacheStore(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	client := &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		call := calls.Add(1)
		if call == 1 {
			close(started)
			<-release
		}
		body, _ := json.Marshal(map[string]any{"streams": []map[string]string{{
			"url": "https://stream.example/" + r.URL.Host + ".mkv",
		}}})
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(body)), Header: make(http.Header)}, nil
	})}
	resolver := &manifestStreamResolver{client: client}
	resolver.Configure(resolverConfig{ManifestURL: "https://old.example/manifest.json"})
	done := make(chan error, 1)
	go func() {
		_, _, _, err := resolver.GetCandidates(context.Background(), "virtual://movie/tt0133093")
		done <- err
	}()
	<-started
	resolver.Configure(resolverConfig{ManifestURL: "https://new.example/manifest.json"})
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("in-flight GetCandidates() error = %v", err)
	}
	candidates, _, _, err := resolver.GetCandidates(context.Background(), "virtual://movie/tt0133093")
	if err != nil {
		t.Fatalf("GetCandidates() after Configure error = %v", err)
	}
	if calls.Load() != 2 || len(candidates) != 1 || !strings.Contains(candidates[0].URL, "new.example") {
		t.Fatalf("calls=%d candidates=%#v, want a fresh new-provider response", calls.Load(), candidates)
	}
}

func TestCandidateCacheEnforcesAggregateByteLimit(t *testing.T) {
	resolver := &manifestStreamResolver{}
	resolver.Configure(resolverConfig{})
	resolver.mu.RLock()
	generation := resolver.generation
	resolver.mu.RUnlock()
	now := time.Now()
	for i := 0; i < 32; i++ {
		resolver.storeCandidateCache(fmt.Sprintf("movie|tt%d", i), []StreamCandidate{{
			URL:  fmt.Sprintf("https://stream.example/%d.mkv", i),
			Name: strings.Repeat("x", 1<<20),
		}}, now.Add(time.Hour), now.Add(time.Duration(i)*time.Second), generation)
	}
	resolver.cacheMu.Lock()
	defer resolver.cacheMu.Unlock()
	if resolver.cacheBytes > maxCandidateCacheBytes {
		t.Fatalf("cache bytes = %d, limit = %d", resolver.cacheBytes, maxCandidateCacheBytes)
	}
	if len(resolver.cache) >= 32 {
		t.Fatalf("cache retained %d oversized aggregate entries", len(resolver.cache))
	}
}

func TestValidateConnectionRequiresStremioStreamManifest(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr bool
	}{
		{name: "valid string resource", body: `{"id":"provider","resources":["stream"],"types":["movie","series"]}`},
		{name: "valid object resource", body: `{"id":"provider","resources":[{"name":"stream"}],"types":["movie"]}`},
		{name: "html", body: `<html>not a manifest</html>`, wantErr: true},
		{name: "missing stream", body: `{"id":"provider","resources":["catalog"],"types":["movie"]}`, wantErr: true},
		{name: "unsupported types", body: `{"id":"provider","resources":["stream"],"types":["music"]}`, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resolver := &manifestStreamResolver{client: &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(test.body)), Header: make(http.Header)}, nil
			})}}
			resolver.Configure(resolverConfig{ManifestURL: "https://provider.example/manifest.json"})
			err := resolver.ValidateConnection(context.Background())
			if (err != nil) != test.wantErr {
				t.Fatalf("ValidateConnection() error = %v, wantErr %v", err, test.wantErr)
			}
		})
	}
}

func TestEmbeddedManifestUsesTypedVirtualStreamDescriptor(t *testing.T) {
	manifest, err := publicmanifest.Load(manifestJSON)
	if err != nil {
		t.Fatalf("Load(manifestJSON) error = %v", err)
	}
	for _, descriptor := range manifest.GetCapabilities() {
		if descriptor.GetType() == "virtual_stream_provider.v1" {
			if descriptor.GetVirtualStreamProvider() == nil {
				t.Fatal("virtual stream provider is missing its typed descriptor")
			}
			return
		}
	}
	t.Fatal("virtual stream provider capability is missing")
}

func TestRuntimeConfigureDoesNotPartiallyApplyOnFailure(t *testing.T) {
	resolver := &manifestStreamResolver{}
	resolver.Configure(resolverConfig{ManifestURL: "https://old.example/manifest.json"})
	monitor := newMediaMonitor(resolver, nil)
	server := &runtimeServer{resolver: resolver, monitor: monitor}
	value, err := structpb.NewStruct(map[string]any{
		"manifest_url":      "https://new.example/manifest.json",
		"movie_library_id":  1,
		"series_library_id": 2,
		"monitor_file":      t.TempDir() + "/queue.json",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = server.Configure(context.Background(), &pb.ConfigureRequest{Config: []*pb.ConfigEntry{{Key: configKey, Value: value}}})
	if err == nil {
		t.Fatal("Configure() succeeded without a bound RuntimeHost")
	}
	resolver.mu.RLock()
	got := resolver.config.ManifestURL
	resolver.mu.RUnlock()
	if got != "https://old.example/manifest.json" {
		t.Fatalf("resolver config = %q after rejected Configure, want old config", got)
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

func TestVirtualPathMetadataPreservesSelectionAndBindsIdentity(t *testing.T) {
	request := &pb.ResolveVirtualStreamRequest{MediaType: "movie", ExternalIds: map[string]string{"imdb": "tt0133093"}}
	metadata, err := structpb.NewStruct(map[string]any{"virtual_uri": "virtual://movie/tt0133093?profile=1080p&results=all"})
	if err != nil {
		t.Fatal(err)
	}
	request.Metadata = metadata
	path, err := virtualPathForResolveRequest(request)
	if err != nil || path != "virtual://movie/tt0133093?profile=1080p&results=all" {
		t.Fatalf("path=%q err=%v", path, err)
	}
	metadata, _ = structpb.NewStruct(map[string]any{"virtual_uri": "virtual://movie/tt9999999?results=all"})
	request.Metadata = metadata
	if _, err := virtualPathForResolveRequest(request); err == nil {
		t.Fatal("mismatched metadata virtual_uri accepted")
	}
	metadata, _ = structpb.NewStruct(map[string]any{"virtual_uri": "not-a-virtual-uri"})
	request.Metadata = metadata
	if _, err := virtualPathForResolveRequest(request); err == nil {
		t.Fatal("malformed metadata virtual_uri accepted")
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
	if err != nil || url5 != "https://stream.example/movie3.mkv" {
		t.Fatalf("Expected ranked movie3.mkv (fallback any stream), got %v %v", url5, err)
	}

	variants := resolver.GetVariants(ctx, "virtual://movie/tt0133093")
	if len(variants) != 5 {
		t.Fatalf("Expected one version per profile plus More results, got %d", len(variants))
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
	if err != nil || url6 != "https://stream.example/movie3.mkv" {
		t.Fatalf("Expected ranked movie3.mkv when profiles are disabled, got %v %v", url6, err)
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

	ordered := QualityConfig{EnableProfiles: true, Profiles: []QualityProfile{
		{Label: "Unspecified"}, {Label: "Second", PreferredOrder: 2}, {Label: "First", PreferredOrder: 1},
	}}
	if err := ordered.Validate(); err != nil {
		t.Fatal(err)
	}
	if got := []string{ordered.Profiles[0].Label, ordered.Profiles[1].Label, ordered.Profiles[2].Label}; got[0] != "First" || got[1] != "Second" || got[2] != "Unspecified" {
		t.Fatalf("profile order = %v", got)
	}
}

func TestQualityProfilesAlwaysMaterializeConfiguredVariants(t *testing.T) {
	resolver := &manifestStreamResolver{}
	resolver.Configure(resolverConfig{Quality: QualityConfig{
		EnableProfiles: true,
		Profiles:       []QualityProfile{{Label: "1080p"}},
	}})
	if got := resolver.GetConfiguredVariants("virtual://movie/tt0133093"); len(got) != 2 {
		t.Fatalf("configured variants = %d, want 2 (profile plus More results)", len(got))
	}
}

func TestProfilesDisabledExposeDefaultAndMoreResults(t *testing.T) {
	resolver := &manifestStreamResolver{}
	resolver.Configure(resolverConfig{Quality: QualityConfig{EnableProfiles: false}})
	variants := resolver.GetConfiguredVariants("virtual://movie/tt0133093")
	if len(variants) != 2 {
		t.Fatalf("configured variants = %d, want Default plus More results", len(variants))
	}
	if variants[0].Label != "Default" || variants[0].VirtualURI != "virtual://movie/tt0133093" {
		t.Fatalf("default variant = %#v", variants[0])
	}
	if variants[1].Label != "More results…" || !strings.Contains(variants[1].VirtualURI, "results=all") {
		t.Fatalf("more-results variant = %#v", variants[1])
	}
}

func TestDecodeQualityProfilesRequiresTypedArray(t *testing.T) {
	raw := []any{map[string]any{"label": "1080p", "resolution": "1080p", "preferred_order": float64(2)}}
	profiles, err := decodeQualityProfiles(raw)
	if err != nil {
		t.Fatalf("decodeQualityProfiles() error = %v", err)
	}
	if len(profiles) != 1 || profiles[0].Label != "1080p" || profiles[0].PreferredOrder != 2 {
		t.Fatalf("profiles = %#v", profiles)
	}
	if _, err := decodeQualityProfiles(`[{"label":"1080p"}]`); err == nil {
		t.Fatal("JSON string quality profiles accepted")
	}
}

func TestProviderHTTPClientRejectsCrossOriginRedirect(t *testing.T) {
	client := newProviderHTTPClient()
	origin, _ := http.NewRequest(http.MethodGet, "https://provider.example/token/manifest.json", nil)
	target, _ := http.NewRequest(http.MethodGet, "https://attacker.example/token/manifest.json", nil)
	if err := client.CheckRedirect(target, []*http.Request{origin}); err == nil {
		t.Fatal("cross-origin redirect accepted")
	}
	sameOrigin, _ := http.NewRequest(http.MethodGet, "https://provider.example/token/next.json", nil)
	if err := client.CheckRedirect(sameOrigin, []*http.Request{origin}); err != nil {
		t.Fatalf("same-origin redirect rejected: %v", err)
	}
}

func TestDecodeBoundedJSONRejectsOversizedResponse(t *testing.T) {
	var payload stremioResponse
	if err := decodeBoundedJSON(strings.NewReader(`{"streams":[]}`), 64, &payload); err != nil {
		t.Fatalf("small response rejected: %v", err)
	}
	if err := decodeBoundedJSON(strings.NewReader(`{"streams":[]}`+strings.Repeat(" ", 64)), 64, &payload); err == nil {
		t.Fatal("oversized response accepted")
	}
}

func TestCandidateDisplayNameIncludesProviderSize(t *testing.T) {
	name := candidateDisplayName(StreamCandidate{Name: "AltMount FHD", Title: "Movie 1080p WEB-DL 5.69 GB"})
	if !strings.Contains(name, "5.69 GB") {
		t.Fatalf("candidate display name = %q, want provider size", name)
	}
}

func TestCandidateVariantIDIgnoresRotatingQueryCredentials(t *testing.T) {
	base := StreamCandidate{
		Name: "AltMount 4K", Title: "Movie 2160p WEB-DL", Resolution: "2160p",
		SourceType: "web-dl", OriginalIndex: 0,
		URL: "https://provider.example/play/movie.mkv?token=old&expires=100",
	}
	rotated := base
	rotated.URL = "https://provider.example/play/movie.mkv?token=new&expires=200"
	if got, want := candidateVariantID(rotated), candidateVariantID(base); got != want {
		t.Fatalf("rotating URL credentials changed candidate ID: got %q want %q", got, want)
	}
}

func TestCandidateVariantIDIgnoresProviderOrder(t *testing.T) {
	first := StreamCandidate{Name: "1080p WEB-DL", Resolution: "1080p", OriginalIndex: 0, URL: "https://provider.example/a.mkv?token=one"}
	second := first
	second.OriginalIndex = 1
	if got, want := candidateVariantID(second), candidateVariantID(first); got != want {
		t.Fatalf("provider reorder changed candidate ID: got %q want %q", got, want)
	}
}

func TestCandidateVariantIDDifferentiatesStableSources(t *testing.T) {
	first := StreamCandidate{Name: "1080p WEB-DL", Resolution: "1080p", URL: "https://provider.example/a.mkv?token=one"}
	second := first
	second.URL = "https://provider.example/b.mkv?token=one"
	if got, want := candidateVariantID(first), candidateVariantID(second); got == want {
		t.Fatalf("distinct stable sources share candidate ID %q", got)
	}
}

func TestSelectCandidatesRanksResultsAndLimitsProfile(t *testing.T) {
	resolver := &manifestStreamResolver{}
	resolver.Configure(resolverConfig{Quality: QualityConfig{EnableProfiles: true, Profiles: []QualityProfile{
		{Label: "4K", Resolution: "2160p"},
		{Label: "SD", Resolution: "480p"},
	}, FallbackToAnyStream: true}})
	candidates := []StreamCandidate{
		{Title: "720p HDTV", Resolution: "720p", SourceType: "hdtv", OriginalIndex: 0, URL: "https://stream.example/720.mkv"},
		{Title: "2160p WEB-DL", Resolution: "2160p", SourceType: "web-dl", OriginalIndex: 1, URL: "https://stream.example/2160.mkv"},
		{Title: "1080p REMUX", Resolution: "1080p", SourceType: "remux", OriginalIndex: 2, URL: "https://stream.example/1080.mkv"},
	}
	profile := resolver.SelectCandidates("virtual://movie/tt1?profile=4K", candidates)
	if len(profile) != 1 || profile[0].Resolution != "2160p" {
		t.Fatalf("profile candidates = %#v, want one 2160p candidate", profile)
	}
	all := resolver.SelectCandidates("virtual://movie/tt1?results=all", candidates)
	if len(all) != 3 || all[0].Resolution != "2160p" || all[1].Resolution != "1080p" || all[2].Resolution != "720p" {
		t.Fatalf("all candidates order = %#v, want 2160p,1080p,720p", all)
	}
	defaultResult := resolver.SelectCandidates("virtual://movie/tt1", candidates)
	if len(defaultResult) != 1 || defaultResult[0].Resolution != "2160p" {
		t.Fatalf("default candidates = %#v, want one ranked 2160p candidate", defaultResult)
	}
	fallback := resolver.SelectCandidates("virtual://movie/tt1?profile=Unknown", candidates)
	if len(fallback) != 1 || fallback[0].Resolution != "2160p" {
		t.Fatalf("fallback candidates = %#v, want one ranked 2160p candidate", fallback)
	}
	knownFallback := resolver.SelectCandidates("virtual://movie/tt1?profile=SD", candidates)
	if len(knownFallback) != 1 || knownFallback[0].Resolution != "2160p" {
		t.Fatalf("known-profile fallback candidates = %#v, want one ranked 2160p candidate", knownFallback)
	}
	explicitID := candidateVariantID(candidates[0])
	explicit := resolver.SelectCandidates("virtual://movie/tt1?result="+url.QueryEscape(explicitID), candidates)
	if len(explicit) != 1 || explicit[0].Resolution != "720p" {
		t.Fatalf("explicit candidates = %#v, want exact 720p candidate", explicit)
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

func TestParseStreamDetailsUsesBoundedAudioAndHDRMarkers(t *testing.T) {
	falsePositive := StreamCandidate{Title: "Added Value Adventure", URL: "https://provider.example/video.mkv"}
	parseStreamDetails(&falsePositive)
	if falsePositive.CodecAudio != "" || falsePositive.HDR != "" {
		t.Fatalf("false-positive metadata = audio %q HDR %q", falsePositive.CodecAudio, falsePositive.HDR)
	}

	actual := StreamCandidate{Title: "2160p DV E-AC-3 Atmos"}
	parseStreamDetails(&actual)
	if actual.HDR != "dv" || actual.CodecAudio != "atmos" {
		t.Fatalf("parsed metadata = audio %q HDR %q", actual.CodecAudio, actual.HDR)
	}
}
