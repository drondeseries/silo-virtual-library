package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func testSearchResults() []prowlarrRelease {
	return []prowlarrRelease{
		{GUID: "abc123", Title: "Obsession.2026.2160p.WEB-DL.DDP5.1.HDR.x265", IMDbID: 0, TMDBID: 0, TVDBID: 0, Indexer: "altHUB"},
		{GUID: "def456", Title: "Soul.2020.1080p.BluRay.x264-SPARKS", IMDbID: 0, TMDBID: 0, TVDBID: 0, Indexer: "seedpool"},
		{GUID: "ghi789", Title: "The.Matrix.1999.2160p.WEB-DL.DDP.5.1.H.265-NOGRP", IMDbID: 133093, TMDBID: 603, TVDBID: 0, Indexer: "seedpool"},
		{GUID: "jkl012", Title: "Breaking.Bad.S01E01.720p.BluRay.DD5.1.x264", IMDbID: 0, TMDBID: 0, TVDBID: 0, Indexer: "NZBgeek"},
	}
}

func TestParseProwlarrSearch(t *testing.T) {
	// Empty body should error (well, return empty slice actually — let's test valid)
	data, _ := json.Marshal(testSearchResults())
	releases, err := parseProwlarrSearch(&stringReader{data: string(data), pos: 0})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(releases) != 4 {
		t.Fatalf("got %d releases, want 4", len(releases))
	}
	if releases[0].Title != "Obsession.2026.2160p.WEB-DL.DDP5.1.HDR.x265" {
		t.Fatalf("wrong title: %q", releases[0].Title)
	}
}

func TestEpisodeReleaseKeysRecognizeSupportedFormats(t *testing.T) {
	tests := []struct {
		title string
		want  bool
	}{
		{"Show.S02E03.1080p.WEB-DL", true},
		{"Show.2x03.720p.WEB", true},
		{"Show.Season 2 Episode 3.HDTV", true},
		{"Show.S02E04.1080p.WEB-DL", false},
	}
	for _, test := range tests {
		t.Run(test.title, func(t *testing.T) {
			if got := containsEpisodeReleaseKey(test.title, 2, 3); got != test.want {
				t.Fatalf("containsEpisodeReleaseKey() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestPrepareProwlarrReleaseCachesDerivedMetadata(t *testing.T) {
	release := prowlarrRelease{Title: "Show.S02E03.2160p.HDR.x265"}
	prepareProwlarrRelease(&release)
	if release.parsedCandidate == nil {
		t.Fatal("expected parsed candidate")
	}
	if release.parsedCandidate.Resolution != "2160p" {
		t.Fatalf("resolution = %q, want 2160p", release.parsedCandidate.Resolution)
	}
	if !containsEpisodeReleaseKey(release.Title, 2, 3) {
		t.Fatal("expected cached episode key to match")
	}
}

func TestNormalizeReleaseTitle(t *testing.T) {
	tests := map[string]struct {
		wantTitle string
		wantYear  int
	}{
		"Obsession.2026.2160p.WEB-DL.DDP5.1.HDR.x265":              {wantTitle: "obsession", wantYear: 2026},
		"The.Matrix.1999.2160p.WEB-DL.DDP.5.1.H.265":               {wantTitle: "the matrix", wantYear: 1999},
		"Soul.2020.1080p.BluRay.x264-SPARKS":                       {wantTitle: "soul", wantYear: 2020},
		"Breaking.Bad.S01E01.720p.BluRay.DD5.1.x264":               {wantTitle: "breaking bad", wantYear: 0},
		"Dune.Part.Two.2024.2160p.WEBRip.DDP5.1.Atmos.DV.HDR.x265": {wantTitle: "dune part two", wantYear: 2024},
	}
	for raw, want := range tests {
		gotTitle, gotYear := normalizeReleaseTitle(raw)
		if gotTitle != want.wantTitle || gotYear != want.wantYear {
			t.Errorf("normalizeReleaseTitle(%q) = (%q, %d), want (%q, %d)",
				raw, gotTitle, gotYear, want.wantTitle, want.wantYear)
		}
	}
}

func TestMatchByTitleAndYear(t *testing.T) {
	cache := searchCacheFromTest(t, testSearchResults())

	item := monitoredMedia{Title: "Obsession", Year: 2026, MediaType: "movie"}
	if !cache.Match(item) {
		t.Fatal("expected match for Obsession 2026")
	}

	item = monitoredMedia{Title: "The Matrix", Year: 1999, MediaType: "movie"}
	if !cache.Match(item) {
		t.Fatal("expected match for The Matrix 1999")
	}

	item = monitoredMedia{Title: "Soul", Year: 2020, MediaType: "movie"}
	if !cache.Match(item) {
		t.Fatal("expected match for Soul 2020")
	}
}

func TestMatchByIMDbID(t *testing.T) {
	cache := searchCacheFromTest(t, testSearchResults())

	// The Matrix has IMDbID 133093 in test data
	item := monitoredMedia{Title: "The Matrix", IMDbID: "133093", Year: 1999, MediaType: "movie"}
	if !cache.Match(item) {
		t.Fatal("expected IMDb ID match for The Matrix (133093)")
	}
}

func TestMatchWrongYearMisses(t *testing.T) {
	cache := searchCacheFromTest(t, testSearchResults())

	item := monitoredMedia{Title: "Obsession", Year: 2025, MediaType: "movie"}
	if cache.Match(item) {
		t.Fatal("should not match Obsession 2025 against 2026 feed entry")
	}
}

func TestMatchEmptyResultsReturnsFalse(t *testing.T) {
	cache := newProwlarrSearchClient(nil)
	item := monitoredMedia{Title: "Obsession", Year: 2026, MediaType: "movie"}
	if cache.Match(item) {
		t.Fatal("empty results should not match")
	}
}

func TestSearchURLConstruction(t *testing.T) {
	cache := newProwlarrSearchClient(nil)
	cache.Configure("https://prowlarr.example.com", "mykey", 15)
	u, err := cache.searchURL()
	if err != nil {
		t.Fatalf("searchURL: %v", err)
	}
	if u == "" {
		t.Fatal("expected non-empty URL")
	}
}

func TestSearchURLConstructionForTitle(t *testing.T) {
	cache := newProwlarrSearchClient(nil)
	cache.Configure("https://prowlarr.example.com", "mykey", 15)
	u, err := cache.searchURLForQuery("Lucky Strike")
	if err != nil {
		t.Fatalf("searchURLForQuery: %v", err)
	}
	parsed, err := url.Parse(u)
	if err != nil {
		t.Fatalf("parse search URL: %v", err)
	}
	if got := parsed.Query().Get("query"); got != "Lucky Strike" {
		t.Fatalf("query = %q, want Lucky Strike", got)
	}
}

func TestRefreshFetchesSearch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(testSearchResults())
	}))
	defer srv.Close()

	cache := newProwlarrSearchClient(srv.Client())
	cache.Configure(srv.URL, "", 15)

	ctx := context.Background()
	if err := cache.refresh(ctx); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if cache.Stale() {
		t.Fatal("should not be stale after refresh")
	}

	item := monitoredMedia{Title: "Obsession", Year: 2026, MediaType: "movie"}
	if !cache.Match(item) {
		t.Fatal("expected match after refresh")
	}
}

func TestRefreshErrorDoesNotClobberExistingItems(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(testSearchResults())
	}))
	defer srv.Close()

	cache := newProwlarrSearchClient(srv.Client())
	cache.Configure(srv.URL, "", 15)

	ctx := context.Background()
	if err := cache.refresh(ctx); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	// Close the server so next refresh fails
	srv.Close()

	// Same URL — items should NOT be cleared
	cache.Configure(srv.URL, "", 15)

	cache.mu.Lock()
	hasItems := len(cache.releases) > 0
	cache.mu.Unlock()
	if !hasItems {
		t.Fatal("Configure with same URL cleared releases")
	}

	if err := cache.refresh(ctx); err == nil {
		t.Fatal("expected error for dead server")
	}

	item := monitoredMedia{Title: "Obsession", Year: 2026, MediaType: "movie"}
	if !cache.Match(item) {
		t.Fatal("releases should survive a failed refresh")
	}
}

func TestProwlarrIndexRoundTripsBeyondSearchResponseLimit(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "index.json")
	releases := make([]prowlarrRelease, 0, 5000)
	for i := 0; i < cap(releases); i++ {
		releases = append(releases, prowlarrRelease{
			GUID:        "guid-" + strings.Repeat("x", 1500) + string(rune(i)),
			Title:       "Test release",
			DownloadURL: "https://example.test/" + strings.Repeat("y", 1500),
		})
	}
	data, _ := json.Marshal(releases)
	if len(data) <= maxSearchBodyBytes {
		t.Fatalf("test fixture is only %d bytes, want more than %d", len(data), maxSearchBodyBytes)
	}

	if err := saveProwlarrIndex(path, releases); err != nil {
		t.Fatalf("save index: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat index: %v", err)
	}
	if info.Size() > maxProwlarrIndexBytes {
		t.Fatalf("index size = %d, exceeds %d", info.Size(), maxProwlarrIndexBytes)
	}

	cache := newProwlarrSearchClient(nil)
	if err := cache.ConfigureIndexFile(path); err != nil {
		t.Fatalf("load index: %v", err)
	}
	cache.mu.Lock()
	loaded := len(cache.releases)
	cache.mu.Unlock()
	if loaded == 0 {
		t.Fatal("expected persisted releases to load")
	}
}

func TestProwlarrIndexRejectsHardLimit(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "index.json")
	if err := os.WriteFile(path, []byte(strings.Repeat("x", maxProwlarrIndexBytes+1)), 0600); err != nil {
		t.Fatalf("write index: %v", err)
	}

	cache := newProwlarrSearchClient(nil)
	err := cache.ConfigureIndexFile(path)
	if err == nil || !strings.Contains(err.Error(), "Prowlarr index exceeds") {
		t.Fatalf("ConfigureIndexFile error = %v, want size error", err)
	}
}

func TestMinIntervalEnforcement(t *testing.T) {
	cache := newProwlarrSearchClient(nil)
	cache.Configure("https://example.com", "", 1) // below minimum
	if cache.interval != time.Duration(defaultSearchCheckMinutes)*time.Minute {
		t.Fatalf("interval should be %d min, got %v", defaultSearchCheckMinutes, cache.interval)
	}

	cache.Configure("https://example.com", "", maxSearchCheckMinutes+1)
	if cache.interval != time.Duration(maxSearchCheckMinutes)*time.Minute {
		t.Fatalf("interval should be %d min, got %v", maxSearchCheckMinutes, cache.interval)
	}
}

func searchCacheFromTest(t *testing.T, releases []prowlarrRelease) *prowlarrSearchClient {
	t.Helper()
	cache := newProwlarrSearchClient(nil)
	cache.mu.Lock()
	cache.releases = releases
	cache.lastFetch = time.Now()
	cache.mu.Unlock()
	return cache
}

// stringReader is a simple io.Reader for testing.
type stringReader struct {
	data string
	pos  int
}

func (r *stringReader) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n = copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

func TestProwlarrAuthUsesHeaderNotQuery(t *testing.T) {
	client := newProwlarrSearchClient(nil)
	client.Configure("http://prowlarr.local:9696", "super-secret-key", 15)

	endpoint, err := client.searchURLForQuery("the matrix")
	if err != nil {
		t.Fatalf("searchURLForQuery: %v", err)
	}
	if strings.Contains(endpoint, "apikey") || strings.Contains(endpoint, "super-secret-key") {
		t.Fatalf("search URL embeds credentials: %s", endpoint)
	}

	req, err := client.newSearchRequest(context.Background(), endpoint)
	if err != nil {
		t.Fatalf("newSearchRequest: %v", err)
	}
	if got := req.Header.Get("X-Api-Key"); got != "super-secret-key" {
		t.Fatalf("X-Api-Key header = %q, want configured key", got)
	}
	if strings.Contains(req.URL.RawQuery, "apikey") {
		t.Fatalf("query still carries apikey: %s", req.URL.RawQuery)
	}
}

func TestProwlarrTransportErrorHidesURLAndKey(t *testing.T) {
	failing := &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return nil, &url.Error{Op: "Get", URL: "http://prowlarr.local:9696/api/v1/search?apikey=leaked-key", Err: context.DeadlineExceeded}
	})}
	client := newProwlarrSearchClient(failing)
	client.Configure("http://prowlarr.local:9696", "leaked-key", 15)

	item := monitoredMedia{Title: "The Matrix", MediaType: "movie"}
	_, err := client.search(context.Background(), item)
	if err == nil {
		t.Fatal("expected transport error")
	}
	message := err.Error()
	if strings.Contains(message, "prowlarr.local") || strings.Contains(message, "leaked-key") || strings.Contains(message, "/api/") {
		t.Fatalf("transport error leaks request details: %q", message)
	}
	if !strings.Contains(strings.ToLower(message), "prowlarr search request failed") {
		t.Fatalf("unexpected sanitized error text: %q", message)
	}
}

func TestRedactErrorStripsCredentials(t *testing.T) {
	input := `Get "https://prowlarr/api/v1/search?apikey=abc123&query=x&token=tok456&password=pw789": context deadline exceeded`
	got := redactError(errors.New(input))
	for _, secret := range []string{"abc123", "tok456", "pw789"} {
		if strings.Contains(got, secret) {
			t.Fatalf("redactError leaked %q in: %s", secret, got)
		}
	}
	for _, kept := range []string{"api/v1/search", "query=x"} {
		if !strings.Contains(got, kept) {
			t.Fatalf("redactError dropped useful context %q from: %s", kept, got)
		}
	}
	if redactError(nil) != "" {
		t.Fatal("redactError(nil) should be empty")
	}
}

func TestSortProwlarrReleasesByCustomFormats(t *testing.T) {
	formats := []CustomFormat{
		{Name: "Preferred", Regex: `(?i)\b(?:Preferred|TopGroup)\b`, Score: 500},
		{Name: "Fallback", Regex: `(?i)\bFallback\b`, Score: 100},
		{Name: "Dubbed", Regex: `(?i)\bDubbed\b`, Score: -500},
	}
	releases := []prowlarrRelease{
		{GUID: "dubbed", Title: "Movie.2026.1080p.BluRay.Dubbed.x264", PublishDate: "2026-09-01T10:00:00Z"},
		{GUID: "neutral", Title: "Movie.2026.1080p.BluRay.x264", PublishDate: "2026-09-01T12:00:00Z"},
		{GUID: "fallback", Title: "Movie.2026.1080p.BluRay.Fallback.x264", PublishDate: "2026-09-01T13:00:00Z"},
		{GUID: "pref1080", Title: "Movie.2026.1080p.BluRay.Preferred.x264", PublishDate: "2026-09-01T14:00:00Z"},
		{GUID: "pref2160", Title: "Movie.2026.2160p.BluRay.Preferred.x265", PublishDate: "2026-09-01T15:00:00Z"},
	}

	sortProwlarrReleases(releases, formats)

	if releases[0].GUID != "pref2160" {
		t.Fatalf("expected top release to be pref2160, got %q", releases[0].GUID)
	}
	if releases[1].GUID != "pref1080" {
		t.Fatalf("expected second release to be pref1080, got %q", releases[1].GUID)
	}
	if releases[2].GUID != "fallback" {
		t.Fatalf("expected third release to be fallback, got %q", releases[2].GUID)
	}
	if releases[3].GUID != "neutral" {
		t.Fatalf("expected fourth release to be neutral, got %q", releases[3].GUID)
	}
	if releases[4].GUID != "dubbed" {
		t.Fatalf("expected last release to be dubbed, got %q", releases[4].GUID)
	}
}

func TestMatchProwlarrReleasesWithCustomFormatRejection(t *testing.T) {
	formats := []CustomFormat{
		{Name: "German", Regex: `(?i)\b(?:german|deutsch)\b`, Reject: true},
	}
	quality := QualityConfig{CustomFormats: formats}

	item := monitoredMedia{Title: "Inception", Year: 2010, MediaType: "movie"}

	germanOnly := []prowlarrRelease{
		{GUID: "1", Title: "Inception.2010.1080p.German.DL.x264"},
	}
	if matchProwlarrReleasesWithQuality(germanOnly, item, quality) {
		t.Fatal("expected match to fail when only release is rejected by custom format")
	}

	mixed := []prowlarrRelease{
		{GUID: "1", Title: "Inception.2010.1080p.German.DL.x264"},
		{GUID: "2", Title: "Inception.2010.1080p.BluRay.x264"},
	}
	if !matchProwlarrReleasesWithQuality(mixed, item, quality) {
		t.Fatal("expected match to succeed when a non-rejected release exists")
	}
}

func TestMatchEpisodeWithQualityCustomFormatRejection(t *testing.T) {
	formats := []CustomFormat{
		{Name: "German", Regex: `(?i)\b(?:german|deutsch)\b`, Reject: true},
	}
	quality := QualityConfig{CustomFormats: formats}
	item := monitoredMedia{Title: "Severance", MediaType: "series"}
	ep := virtualEpisode{Season: 1, Episode: 1}

	client := newProwlarrSearchClient(nil)
	client.Configure("https://prowlarr.example.com", "key", 15)

	client.releases = []prowlarrRelease{
		{GUID: "1", Title: "Severance.S01E01.1080p.German.DL.x264"},
	}
	if client.MatchEpisodeWithQuality(item, ep, quality) {
		t.Fatal("expected episode match to fail when only release is rejected by custom format")
	}

	client.releases = []prowlarrRelease{
		{GUID: "1", Title: "Severance.S01E01.1080p.German.DL.x264"},
		{GUID: "2", Title: "Severance.S01E01.1080p.BluRay.x264"},
	}
	if !client.MatchEpisodeWithQuality(item, ep, quality) {
		t.Fatal("expected episode match to succeed when a non-rejected release exists")
	}
}

func TestSortProwlarrReleasesSingleReleasePopulatesQualityScore(t *testing.T) {
	formats := []CustomFormat{
		{Name: "Preferred", Regex: `(?i)\bPreferred\b`, Score: 500},
	}
	releases := []prowlarrRelease{
		{GUID: "single", Title: "Movie.2026.1080p.BluRay.Preferred.x264"},
	}
	sortProwlarrReleases(releases, formats)
	if releases[0].parsedCandidate == nil {
		t.Fatal("expected parsedCandidate to be non-nil")
	}
	if releases[0].parsedCandidate.QualityScore != 500 {
		t.Fatalf("expected QualityScore = 500, got %d", releases[0].parsedCandidate.QualityScore)
	}
}

func TestSortProwlarrReleasesPublishDateRFC3339(t *testing.T) {
	releases := []prowlarrRelease{
		{GUID: "older-string-newer-time", Title: "Movie.2026.1080p.BluRay.x264", PublishDate: "2026-09-01T12:00:00-05:00"},
		{GUID: "newer-string-older-time", Title: "Movie.2026.1080p.BluRay.x264", PublishDate: "2026-09-01T15:00:00Z"},
	}
	sortProwlarrReleases(releases, nil)
	if releases[0].GUID != "older-string-newer-time" {
		t.Fatalf("expected release with newer timestamp to be first, got %s", releases[0].GUID)
	}
}

func TestSortProwlarrReleasesPublishDateRFC3339EdgeCases(t *testing.T) {
	// Valid RFC3339 timestamp must sort before an unparseable string like "yesterday",
	// and leading/trailing whitespace must not break timestamp parsing.
	releases := []prowlarrRelease{
		{GUID: "unparseable", Title: "Movie.2026.1080p.BluRay.x264", PublishDate: "yesterday"},
		{GUID: "valid-spaced", Title: "Movie.2026.1080p.BluRay.x264", PublishDate: " 2026-09-02T10:00:00Z "},
		{GUID: "valid-older", Title: "Movie.2026.1080p.BluRay.x264", PublishDate: "2026-09-01T10:00:00Z"},
	}
	sortProwlarrReleases(releases, nil)
	if releases[0].GUID != "valid-spaced" {
		t.Fatalf("expected valid-spaced (Sep 2) to be first, got %s", releases[0].GUID)
	}
	if releases[1].GUID != "valid-older" {
		t.Fatalf("expected valid-older (Sep 1) to be second, got %s", releases[1].GUID)
	}
	if releases[2].GUID != "unparseable" {
		t.Fatalf("expected unparseable to be last, got %s", releases[2].GUID)
	}
}

func TestReleaseMatchesQualityPreservesCaching(t *testing.T) {
	profiles := []QualityProfile{
		{Label: "1080p", Resolution: "1080p"},
	}
	rel := prowlarrRelease{Title: "Show.S01E01.1080p.WEB-DL.x264"}
	if rel.parsedCandidate != nil {
		t.Fatal("expected parsedCandidate to start nil")
	}
	matches := releaseMatchesQuality(&rel, profiles)
	if !matches {
		t.Fatal("expected release to match 1080p profile")
	}
	if rel.parsedCandidate == nil {
		t.Fatal("expected releaseMatchesQuality to cache parsedCandidate on caller release")
	}
	if rel.normalizedTitle == "" {
		t.Fatal("expected releaseMatchesQuality to cache normalizedTitle on caller release")
	}
	if len(rel.episodeKeys) == 0 {
		t.Fatal("expected releaseMatchesQuality to cache episodeKeys on caller release")
	}
}


