package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

const testNewznabFeed = `<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:newznab="http://www.newznab.com/DTD/2010/feeds/attributes/">
<channel>
<item>
<title>Obsession.2026.2160p.WEB-DL.DDP5.1.HDR.x265</title>
<link>https://example.com/1</link>
<guid>abc123</guid>
<pubDate>Sun, 01 Jun 2025 12:00:00 GMT</pubDate>
<newznab:attr name="category" value="2000"/>
<newznab:attr name="size" value="10737418240"/>
<newznab:attr name="year" value="2026"/>
</item>
<item>
<title>Soul.2020.1080p.BluRay.x264-SPARKS</title>
<link>https://example.com/2</link>
<guid>def456</guid>
<pubDate>Sat, 15 Aug 2020 08:00:00 GMT</pubDate>
<newznab:attr name="category" value="2000"/>
<newznab:attr name="size" value="5368709120"/>
</item>
<item>
<title>The.Matrix.1999.2160p.WEB-DL.DDP.5.1.H.265-NOGRP</title>
<link>https://example.com/3</link>
<guid>ghi789</guid>
<pubDate>Wed, 01 Jan 2025 00:00:00 GMT</pubDate>
<newznab:attr name="category" value="2000"/>
<newznab:attr name="year" value="1999"/>
</item>
<item>
<title>Breaking.Bad.S01E01.720p.BluRay.DD5.1.x264</title>
<link>https://example.com/4</link>
<guid>jkl012</guid>
<pubDate>Mon, 20 Jan 2014 00:00:00 GMT</pubDate>
<newznab:attr name="category" value="5000"/>
<newznab:attr name="season" value="1"/>
<newznab:attr name="episode" value="1"/>
</item>
</channel>
</rss>`

func TestParseNewznabFeed(t *testing.T) {
	items, err := parseNewznabFeed(
		http.NoBody, // will error — use bytes instead
	)
	if err == nil {
		t.Fatal("expected error for empty body")
	}

	items, err = parseNewznabFeed(
		readTestFeed(t, testNewznabFeed),
	)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(items) != 4 {
		t.Fatalf("got %d items, want 4", len(items))
	}
	if items[0].Title != "Obsession.2026.2160p.WEB-DL.DDP5.1.HDR.x265" {
		t.Fatalf("wrong title: %q", items[0].Title)
	}
	if items[0].attr("category") != "2000" {
		t.Fatalf("wrong category: %q", items[0].attr("category"))
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
		"Breaking.Bad.S01E01.720p.BluRay.DD5.1.x264":               {wantTitle: "breaking bad s01e01", wantYear: 0},
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

func TestMatchYearAndTitle(t *testing.T) {
	cache := rssFeedCacheFromTest(t, testNewznabFeed)

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

func TestMatchWrongYearMisses(t *testing.T) {
	cache := rssFeedCacheFromTest(t, testNewznabFeed)

	item := monitoredMedia{Title: "Obsession", Year: 2025, MediaType: "movie"}
	if cache.Match(item) {
		t.Fatal("should not match Obsession 2025 against 2026 feed entry")
	}
}

func TestMatchNoYearRequiresLongTitle(t *testing.T) {
	cache := rssFeedCacheFromTest(t, testNewznabFeed)

	// Short title without year: should NOT match (avoid false positives)
	item := monitoredMedia{Title: "Soul", Year: 0, MediaType: "movie"}
	if cache.Match(item) {
		t.Fatal("short title without year should not match")
	}

	// Long title without year: should match
	item = monitoredMedia{Title: "The Matrix Reloaded", Year: 0, MediaType: "movie"}
	if !cache.Match(item) {
		t.Fatal("long title 'The Matrix Reloaded' without year should match against 'The.Matrix.1999'")
	}
}

func TestMatchEmptyFeedReturnsFalse(t *testing.T) {
	cache := newRSSFeedCache(nil)
	item := monitoredMedia{Title: "Obsession", Year: 2026, MediaType: "movie"}
	if cache.Match(item) {
		t.Fatal("empty feed should not match")
	}
}

func TestFeedURLConstruction(t *testing.T) {
	cache := newRSSFeedCache(nil)
	cache.Configure("https://prowlarr.example.com/4/api", "mykey", 15)
	u, err := cache.feedURL()
	if err != nil {
		t.Fatalf("feedURL: %v", err)
	}
	if u == "" {
		t.Fatal("expected non-empty URL")
	}
}

func TestRefreshFetchesFeed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/rss+xml")
		w.Write([]byte(testNewznabFeed))
	}))
	defer srv.Close()

	cache := newRSSFeedCache(srv.Client())
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
		w.Header().Set("Content-Type", "application/rss+xml")
		w.Write([]byte(testNewznabFeed))
	}))
	defer srv.Close()

	cache := newRSSFeedCache(srv.Client())
	cache.Configure(srv.URL, "", 15)

	ctx := context.Background()
	if err := cache.refresh(ctx); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	// Close the server so next refresh fails
	srv.Close()

	// Reconfigure with the dead URL — same URL, so items should NOT be cleared
	cache.Configure(srv.URL, "", 15)

	// Verify items survived configure
	cache.mu.Lock()
	hasItems := len(cache.items) > 0
	cache.mu.Unlock()
	if !hasItems {
		t.Fatal("Configure with same URL cleared items")
	}

	if err := cache.refresh(ctx); err == nil {
		t.Fatal("expected error for dead server")
	}

	// Items should survive the failed refresh
	item := monitoredMedia{Title: "Obsession", Year: 2026, MediaType: "movie"}
	if !cache.Match(item) {
		t.Fatal("items should survive a failed refresh")
	}
}

func TestMinIntervalEnforcement(t *testing.T) {
	cache := newRSSFeedCache(nil)
	cache.Configure("https://example.com/api", "", 1) // below minimum
	if cache.interval != time.Duration(defaultRSSCheckMinutes)*time.Minute {
		t.Fatalf("interval should be %d min, got %v", defaultRSSCheckMinutes, cache.interval)
	}

	cache.Configure("https://example.com/api", "", maxRSSCheckMinutes+1)
	if cache.interval != time.Duration(maxRSSCheckMinutes)*time.Minute {
		t.Fatalf("interval should be %d min, got %v", maxRSSCheckMinutes, cache.interval)
	}
}

func rssFeedCacheFromTest(t *testing.T, feed string) *rssFeedCache {
	t.Helper()
	cache := newRSSFeedCache(nil)
	cache.mu.Lock()
	items, err := parseNewznabFeed(readTestFeed(t, feed))
	if err != nil {
		cache.mu.Unlock()
		t.Fatalf("parse: %v", err)
	}
	cache.items = items
	cache.lastFetch = time.Now()
	cache.mu.Unlock()
	return cache
}

func readTestFeed(t *testing.T, data string) *stringReader {
	t.Helper()
	return &stringReader{data: data, pos: 0}
}

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
