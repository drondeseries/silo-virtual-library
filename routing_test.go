package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	pb "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"github.com/hashicorp/go-hclog"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestFulfillCompletesStreamableSeries(t *testing.T) {
	useTestCinemeta(t)
	monitor := newMediaMonitor(resolverFunc(func(context.Context, string) (string, error) {
		return "https://stream.example/video.mkv", nil
	}), hclog.NewNullLogger())
	monitor.Configure(monitorConfig{File: filepath.Join(t.TempDir(), "queue.json")})
	monitor.setRegistrar(registrarFunc(func(context.Context, monitoredMedia) error { return nil }))
	server := &runtimeServer{monitor: monitor}
	response, err := server.Fulfill(context.Background(), fulfillSeriesRequest())
	if err != nil {
		t.Fatal(err)
	}
	if got := response.GetTargets()[0].GetStatus(); got != "completed" {
		t.Fatalf("status = %q, want completed", got)
	}
}

func TestFulfillRegistersVirtualMediaBeforeCompleting(t *testing.T) {
	useTestCinemeta(t)
	monitor := newMediaMonitor(resolverFunc(func(context.Context, string) (string, error) {
		return "https://stream.example/video.mkv", nil
	}), hclog.NewNullLogger())
	monitor.Configure(monitorConfig{File: filepath.Join(t.TempDir(), "queue.json")})
	var registered monitoredMedia
	monitor.setRegistrar(registrarFunc(func(_ context.Context, item monitoredMedia) error {
		registered = item
		return nil
	}))
	server := &runtimeServer{monitor: monitor}
	request := fulfillSeriesRequest()
	request.Connections = []*pb.RouterConnection{{Config: mustStruct(t, map[string]any{"media_folder_id": 42})}}

	response, err := server.Fulfill(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if response.GetTargets()[0].GetStatus() != "completed" {
		t.Fatalf("status = %q", response.GetTargets()[0].GetStatus())
	}
	if registered.Key != "series:tt1234567" || registered.MediaFolderID != 42 || registered.Title != "Example" {
		t.Fatalf("registered media = %+v", registered)
	}
}

func TestSeriesCompletesFromAiredEpisodeMetadataWithoutStreamResult(t *testing.T) {
	useTestCinemeta(t)
	available := false
	monitor := newMediaMonitor(resolverFunc(func(context.Context, string) (string, error) {
		if !available {
			return "", errors.New("not found")
		}
		return "https://stream.example/video.mkv", nil
	}), hclog.NewNullLogger())
	monitor.Configure(monitorConfig{File: filepath.Join(t.TempDir(), "queue.json")})
	monitor.setRegistrar(registrarFunc(func(context.Context, monitoredMedia) error { return nil }))
	server := &runtimeServer{monitor: monitor}
	request := fulfillSeriesRequest()
	response, err := server.Fulfill(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if got := response.GetTargets()[0].GetStatus(); got != "completed" {
		t.Fatalf("status = %q, want completed", got)
	}
	if available {
		t.Fatal("test setup should leave stream unavailable")
	}
}

func TestReleasedMovieCompletesWithoutWaitingForStreamDiscovery(t *testing.T) {
	metadata := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/movie/99/release_dates" {
			_, _ = w.Write([]byte(`{"results":[{"iso_3166_1":"US","release_dates":[{"type":4,"release_date":"2020-01-01T00:00:00.000Z"}]}]}`))
			return
		}
		if r.URL.Path == "/movie/99" {
			_, _ = w.Write([]byte(`{"runtime":133}`))
			return
		}
		_, _ = w.Write([]byte(`{"meta":{"name":"Example Movie","description":"Overview","released":"2019-01-01T00:00:00.000Z"}}`))
	}))
	previousTMDB, previousCinemeta := tmdbBaseURL, cinemetaBaseURL
	tmdbBaseURL, cinemetaBaseURL = metadata.URL, metadata.URL
	t.Cleanup(func() {
		tmdbBaseURL, cinemetaBaseURL = previousTMDB, previousCinemeta
		metadata.Close()
	})

	releasePrewarm := make(chan struct{})
	monitor := newMediaMonitor(resolverFunc(func(context.Context, string) (string, error) {
		<-releasePrewarm
		return "", errors.New("not found")
	}), hclog.NewNullLogger())
	monitor.Configure(monitorConfig{TMDBAPIKey: "test-key", File: filepath.Join(t.TempDir(), "queue.json")})
	var registered monitoredMedia
	monitor.setRegistrar(registrarFunc(func(_ context.Context, item monitoredMedia) error {
		registered = item
		return nil
	}))
	server := &runtimeServer{monitor: monitor}
	response, err := server.Fulfill(context.Background(), &pb.FulfillRequest{
		Request:   &pb.RequestDescriptor{MediaType: "movie", Title: "Example Movie", ExternalIds: map[string]string{"imdb": "tt99", "tmdb": "99"}},
		Qualities: []*pb.RequestedQuality{{Id: "1080p"}},
	})
	if err != nil {
		close(releasePrewarm)
		t.Fatal(err)
	}
	close(releasePrewarm)
	if got := response.GetTargets()[0].GetStatus(); got != "completed" {
		t.Fatalf("status = %q, want completed", got)
	}
	if registered.Runtime != 133 {
		t.Fatalf("registered runtime = %d, want 133", registered.Runtime)
	}
}

func useTestCinemeta(t *testing.T) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"meta":{"name":"Example","description":"Overview","poster":"https://images.example/poster.jpg","videos":[{"id":"tt1234567:1:1","title":"Pilot","season":1,"episode":1,"released":"2020-01-01T00:00:00.000Z"}]}}`))
	}))
	previous := cinemetaBaseURL
	cinemetaBaseURL = server.URL
	t.Cleanup(func() { cinemetaBaseURL = previous; server.Close() })
}

func TestFetchTMDBReleaseUsesEarliestHomeReleaseFromAnyMarket(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"results":[{"iso_3166_1":"FR","release_dates":[{"type":4,"release_date":"2025-01-01T00:00:00.000Z"}]},{"iso_3166_1":"GB","release_dates":[{"type":4,"release_date":"2026-09-01T00:00:00.000Z"}]},{"iso_3166_1":"JP","release_dates":[{"type":4,"release_date":"2026-08-01T00:00:00.000Z"}]}]}`))
	}))
	previous := tmdbBaseURL
	tmdbBaseURL = server.URL
	t.Cleanup(func() { tmdbBaseURL = previous; server.Close() })

	release, err := fetchTMDBRelease(context.Background(), "1", "test-key")
	if err != nil {
		t.Fatal(err)
	}
	if got := release.Format("2006-01-02"); got != "2025-01-01" {
		t.Fatalf("release = %s, want earliest worldwide home-release date", got)
	}
}

func TestFetchTMDBReleaseQueuesWhenAllMarketsAreTheatrical(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"results":[{"iso_3166_1":"US","release_dates":[{"type":3,"release_date":"2026-07-10T00:00:00.000Z"}]},{"iso_3166_1":"FR","release_dates":[{"type":2,"release_date":"2026-07-20T00:00:00.000Z"}]}]}`))
	}))
	previous := tmdbBaseURL
	tmdbBaseURL = server.URL
	t.Cleanup(func() { tmdbBaseURL = previous; server.Close() })

	_, err := fetchTMDBRelease(context.Background(), "1", "test-key")
	if !errors.Is(err, errNoHomeRelease) {
		t.Fatalf("error = %v, want errNoHomeRelease", err)
	}
}

func TestParseRuntimeMinutes(t *testing.T) {
	for input, want := range map[string]int{"48 min": 48, "1h 30min": 90, "129": 129, "": 0} {
		if got := parseRuntimeMinutes(input); got != want {
			t.Errorf("parseRuntimeMinutes(%q) = %d, want %d", input, got, want)
		}
	}
}

type registrarFunc func(context.Context, monitoredMedia) error

func (f registrarFunc) Register(ctx context.Context, item monitoredMedia) error { return f(ctx, item) }

func mustStruct(t *testing.T, value map[string]any) *structpb.Struct {
	t.Helper()
	result, err := structpb.NewStruct(value)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func fulfillSeriesRequest() *pb.FulfillRequest {
	return &pb.FulfillRequest{
		Request:   &pb.RequestDescriptor{MediaType: "series", Title: "Example", ExternalIds: map[string]string{"imdb": "tt1234567"}},
		Qualities: []*pb.RequestedQuality{{Id: "1080p"}},
	}
}
