package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
		// Far-future theatrical dates so this stays valid regardless of when
		// the suite runs; dates within 90 days of "now" would eventually be
		// presumed home releases by design.
		_, _ = w.Write([]byte(`{"results":[{"iso_3166_1":"US","release_dates":[{"type":3,"release_date":"2099-07-10T00:00:00.000Z"}]},{"iso_3166_1":"FR","release_dates":[{"type":2,"release_date":"2099-07-20T00:00:00.000Z"}]}]}`))
	}))
	previous := tmdbBaseURL
	tmdbBaseURL = server.URL
	t.Cleanup(func() { tmdbBaseURL = previous; server.Close() })

	_, err := fetchTMDBRelease(context.Background(), "1", "test-key")
	if !errors.Is(err, errNoHomeRelease) {
		t.Fatalf("error = %v, want errNoHomeRelease", err)
	}
}

func TestMovieReleaseStaysQueuedWhenCinemetaIsDown(t *testing.T) {
	// Point Cinemeta at a closed port to simulate a metadata-provider outage.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	url := server.URL
	server.Close()
	previous := cinemetaBaseURL
	cinemetaBaseURL = url
	t.Cleanup(func() { cinemetaBaseURL = previous })

	monitor := newMediaMonitor(nil, hclog.NewNullLogger())
	item := monitoredMedia{MediaType: "movie", Title: "Old Catalog Film", IMDbID: "tt0100001", Year: 1994}

	releaseDate, err := monitor.movieRelease(context.Background(), item)
	if err == nil {
		t.Fatalf("outage must fail closed, got release date %v", releaseDate)
	}
	if !releaseDate.IsZero() {
		t.Fatalf("outage must not fabricate a release date, got %v", releaseDate)
	}
}

func TestAiredEpisodesExcludesUnknownAndFutureDates(t *testing.T) {
	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	episodes := airedEpisodes([]virtualEpisode{
		{Season: 1, Episode: 1, Released: now.Add(-time.Hour)},
		{Season: 1, Episode: 2, Released: now.Add(time.Hour)},
		{Season: 1, Episode: 3},
		{Season: 0, Episode: 4, Released: now.Add(-time.Hour)},
	}, now)
	if len(episodes) != 1 || episodes[0].Episode != 1 {
		t.Fatalf("aired episodes = %#v, want only the released episode", episodes)
	}
}

func TestMissingEpisodesIncludesKnownUpcomingAndExcludesAvailable(t *testing.T) {
	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	episodes := missingEpisodes([]virtualEpisode{
		{Season: 1, Episode: 1, Released: now.Add(-time.Hour), Available: true},
		{Season: 1, Episode: 2, Released: now.Add(-time.Hour)},
		{Season: 1, Episode: 3, Released: now.Add(time.Hour)},
		{Season: 1, Episode: 4},
	}, now)
	if len(episodes) != 2 || episodes[0].Episode != 2 || episodes[1].Episode != 3 {
		t.Fatalf("missing episodes = %#v, want S01E02 and S01E03", episodes)
	}
}

func TestEpisodeMetadataCompleteRejectsDuplicatesAndInvalidRows(t *testing.T) {
	if episodeMetadataComplete([]virtualEpisode{{Season: 1, Episode: 1, Title: "Pilot"}, {Season: 1, Episode: 1, Title: "Pilot"}}) {
		t.Fatal("duplicate episode coordinates should be incomplete")
	}
	if episodeMetadataComplete([]virtualEpisode{{Season: 0, Episode: 1, Title: "Invalid"}}) {
		t.Fatal("invalid episode coordinates should be incomplete")
	}
}

func TestMergeSeriesEpisodeMetadataPreservesExistingRows(t *testing.T) {
	got := mergeSeriesEpisodeMetadata(
		[]virtualEpisode{{Season: 1, Episode: 1, Title: "Pilot", Runtime: 42}},
		[]virtualEpisode{{Season: 1, Episode: 2, Title: "Second"}},
	)
	if len(got) != 2 || got[0].Episode != 1 || got[1].Episode != 2 {
		t.Fatalf("merged episodes = %#v, want both episodes in order", got)
	}
	if got[0].Runtime != 42 {
		t.Fatalf("existing runtime = %d, want 42", got[0].Runtime)
	}
}

func TestForcedMovieCanPassReleaseGate(t *testing.T) {
	metadata := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/movie/99/release_dates":
			_, _ = w.Write([]byte(`{"results":[{"iso_3166_1":"US","release_dates":[{"type":4,"release_date":"2099-01-01T00:00:00.000Z"}]}]}`))
		case "/movie/99":
			_, _ = w.Write([]byte(`{"runtime":120}`))
		default:
			_, _ = w.Write([]byte(`{"meta":{"name":"Future Movie"}}`))
		}
	}))
	defer metadata.Close()
	previousTMDB, previousCinemeta := tmdbBaseURL, cinemetaBaseURL
	tmdbBaseURL, cinemetaBaseURL = metadata.URL, metadata.URL
	t.Cleanup(func() { tmdbBaseURL, cinemetaBaseURL = previousTMDB, previousCinemeta })

	monitor := newMediaMonitor(nil, hclog.NewNullLogger())
	if err := monitor.Configure(monitorConfig{TMDBAPIKey: "test-key", File: filepath.Join(t.TempDir(), "queue.json")}); err != nil {
		t.Fatal(err)
	}
	item, _, err := monitor.evaluate(context.Background(), monitoredMedia{MediaType: "movie", Title: "Future Movie", TMDBID: "99", Force: true})
	if err != nil {
		t.Fatal(err)
	}
	if !item.Ready {
		t.Fatal("forced movie remained queued")
	}
}

func TestFutureMovieCanBeReleasedByIndexerMatch(t *testing.T) {
	metadata := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/movie/99/release_dates":
			_, _ = w.Write([]byte(`{"results":[{"iso_3166_1":"US","release_dates":[{"type":4,"release_date":"2099-01-01T00:00:00.000Z"}]}]}`))
		case "/movie/99":
			_, _ = w.Write([]byte(`{"runtime":120}`))
		default:
			_, _ = w.Write([]byte(`{"meta":{"name":"Future Movie"}}`))
		}
	}))
	defer metadata.Close()
	previousTMDB, previousCinemeta := tmdbBaseURL, cinemetaBaseURL
	tmdbBaseURL, cinemetaBaseURL = metadata.URL, metadata.URL
	t.Cleanup(func() {
		tmdbBaseURL, cinemetaBaseURL = previousTMDB, previousCinemeta
	})

	monitor := newMediaMonitor(nil, hclog.NewNullLogger())
	monitor.Configure(monitorConfig{TMDBAPIKey: "test-key", File: filepath.Join(t.TempDir(), "queue.json")})
	monitor.prowlarr = newProwlarrSearchClient(nil)
	monitor.prowlarr.Configure("https://prowlarr.example.com", "", 15)
	monitor.prowlarr.releases = []prowlarrRelease{{TMDBID: 99}}
	item, message, err := monitor.evaluate(context.Background(), monitoredMedia{
		MediaType: "movie",
		Title:     "Future Movie",
		TMDBID:    "99",
		IMDbID:    "tt0000099",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !item.Ready {
		t.Fatalf("future movie was not released by indexer match: %s", message)
	}
}

func TestParseRuntimeMinutes(t *testing.T) {
	for input, want := range map[string]int{"48 min": 48, "1h 30min": 90, "129": 129, "": 0} {
		if got := parseRuntimeMinutes(input); got != want {
			t.Errorf("parseRuntimeMinutes(%q) = %d, want %d", input, got, want)
		}
	}
}

func TestMediaMonitorConfigureRejectsInvalidStateWithoutReplacingCurrentState(t *testing.T) {
	dir := t.TempDir()
	validPath := filepath.Join(dir, "valid.json")
	monitor := newMediaMonitor(nil, hclog.NewNullLogger())
	if err := monitor.Configure(monitorConfig{File: validPath}); err != nil {
		t.Fatal(err)
	}
	item := monitoredMedia{Key: "movie:tt1", MediaType: "movie", StreamID: "tt1"}
	if err := monitor.remember(item); err != nil {
		t.Fatal(err)
	}

	badPath := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(badPath, []byte(`[{"Key":"movie:tt2","MediaType":"movie"},{"Key":"movie:tt2","MediaType":"movie"}]`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := monitor.Configure(monitorConfig{File: badPath}); err == nil || !strings.Contains(err.Error(), "duplicate key") {
		t.Fatalf("Configure() error = %v, want duplicate key", err)
	}
	if got, ok := monitor.item(item.Key); !ok || got.StreamID != "tt1" {
		t.Fatalf("valid monitor state replaced after failed configure: %#v, %v", got, ok)
	}
}

func TestMediaMonitorRememberRollsBackWhenPersistenceFails(t *testing.T) {
	monitor := newMediaMonitor(nil, hclog.NewNullLogger())
	if err := monitor.Configure(monitorConfig{File: filepath.Join(t.TempDir(), "missing", "queue.json")}); err != nil {
		t.Fatal(err)
	}
	item := monitoredMedia{Key: "movie:tt1", MediaType: "movie"}
	if err := monitor.remember(item); err == nil {
		t.Fatal("remember succeeded with an unavailable persistence directory")
	}
	if _, ok := monitor.item(item.Key); ok {
		t.Fatal("failed remember left an in-memory item behind")
	}
}

func TestMediaMonitorConfigureRejectsTooManyItems(t *testing.T) {
	items := make([]monitoredMedia, maxMonitoredItems+1)
	for i := range items {
		items[i] = monitoredMedia{Key: fmt.Sprintf("movie:tt%d", i), MediaType: "movie"}
	}
	data, err := json.Marshal(items)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "queue.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	monitor := newMediaMonitor(nil, hclog.NewNullLogger())
	if err := monitor.Configure(monitorConfig{File: path}); err == nil || !strings.Contains(err.Error(), "items") {
		t.Fatalf("Configure() error = %v, want item limit", err)
	}
}

type recordingReconciler struct {
	registerCalls  int
	reconcileCalls int
	registerErr    error
}

func (r *recordingReconciler) Register(context.Context, monitoredMedia) error {
	r.registerCalls++
	return r.registerErr
}

func TestScheduledMonitorSkipsReconcileOnRegistrationFailure(t *testing.T) {
	previousClient := metadataClient
	metadataClient = &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		body := `{"meta":{"name":"Example","videos":[{"id":"tt1234567:1:1","title":"Pilot","season":1,"episode":1,"released":"2020-01-01T00:00:00.000Z"}]}}`
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})}
	t.Cleanup(func() { metadataClient = previousClient })

	monitor := newMediaMonitor(nil, hclog.NewNullLogger())
	if err := monitor.Configure(monitorConfig{File: filepath.Join(t.TempDir(), "queue.json")}); err != nil {
		t.Fatal(err)
	}
	if err := monitor.remember(monitoredMedia{
		Key: "series:tt1234567", MediaType: "series", StreamID: "tt1234567", IMDbID: "tt1234567",
		SourceKey: "request:series:tt1234567",
	}); err != nil {
		t.Fatal(err)
	}
	registrar := &recordingReconciler{registerErr: errors.New("host unavailable")}
	monitor.setRegistrar(registrar)
	server := &runtimeServer{monitor: monitor}
	if _, err := server.Run(context.Background(), &pb.RunScheduledTaskRequest{TaskKey: "monitor-media"}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if registrar.registerCalls != 1 {
		t.Fatalf("register calls = %d, want 1", registrar.registerCalls)
	}
	if registrar.reconcileCalls != 0 {
		t.Fatalf("reconcile calls = %d, want 0 after failed registration", registrar.reconcileCalls)
	}
}

func (r *recordingReconciler) Reconcile(context.Context, string, []string) error {
	r.reconcileCalls++
	return nil
}

func TestScheduledMonitorSkipsRegistrationAndReconcileOnMetadataFailure(t *testing.T) {
	previousClient := metadataClient
	metadataClient = &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("metadata provider unavailable")
	})}
	t.Cleanup(func() { metadataClient = previousClient })

	monitor := newMediaMonitor(nil, hclog.NewNullLogger())
	if err := monitor.Configure(monitorConfig{File: filepath.Join(t.TempDir(), "queue.json")}); err != nil {
		t.Fatal(err)
	}
	item := monitoredMedia{
		Key: "series:tt1234567", MediaType: "series", StreamID: "tt1234567", IMDbID: "tt1234567",
		SourceKey: "request:series:tt1234567", Ready: true,
		Episodes: []virtualEpisode{{Season: 1, Episode: 1, Title: "Pilot", Released: time.Now().Add(-time.Hour)}},
	}
	if err := monitor.remember(item); err != nil {
		t.Fatal(err)
	}
	registrar := &recordingReconciler{}
	monitor.setRegistrar(registrar)
	server := &runtimeServer{monitor: monitor}
	if _, err := server.Run(context.Background(), &pb.RunScheduledTaskRequest{TaskKey: "monitor-media"}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if registrar.registerCalls != 0 {
		t.Fatalf("register calls = %d, want 0 after failed evaluation", registrar.registerCalls)
	}
	if registrar.reconcileCalls != 0 {
		t.Fatalf("reconcile calls = %d, want 0 after failed evaluation", registrar.reconcileCalls)
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
