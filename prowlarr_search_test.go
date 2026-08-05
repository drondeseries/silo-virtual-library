package main

import (
	"context"
	"io"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
