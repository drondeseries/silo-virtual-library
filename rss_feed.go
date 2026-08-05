package main

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultRSSCheckMinutes = 15
	minRSSCheckMinutes     = 15
	maxRSSCheckMinutes     = 10080
	maxRSSFeedBytes        = 8 << 20
)

var rssClient = newRestrictedRedirectHTTPClient(20 * time.Second)

// newznabItem is one release entry from a Newznab/Torznab RSS feed
// (Prowlarr, NZBGet, easyznab). Titles are scene-style, e.g.
// "Obsession.2026.2160p.WEB-DL.DDP5.1.HDR".
type newznabItem struct {
	Title   string `xml:"title"`
	Link    string `xml:"link"`
	GUID    string `xml:"guid"`
	PubDate string `xml:"pubDate"`
	Attrs   []struct {
		Name  string `xml:"name,attr"`
		Value string `xml:"value,attr"`
	} `xml:"attr"`
}

func (i newznabItem) attr(name string) string {
	for _, a := range i.Attrs {
		if strings.EqualFold(a.Name, name) {
			return a.Value
		}
	}
	return ""
}

// rssFeedCache holds the parsed snapshot of the indexer's latest-releases
// feed and refreshes it on a configurable interval (min 15 minutes). The
// queue is matched against this snapshot locally — one feed request per
// interval regardless of how many titles are being monitored.
type rssFeedCache struct {
	mu        sync.Mutex
	url       string
	apiKey    string
	interval  time.Duration
	client    *http.Client
	lastFetch time.Time
	lastErr   error
	items     []newznabItem
}

func newRSSFeedCache(client *http.Client) *rssFeedCache {
	if client == nil {
		client = rssClient
	}
	return &rssFeedCache{client: client}
}

// URL returns the configured indexer RSS URL, or empty string.
func (c *rssFeedCache) URL() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.url
}

func (c *rssFeedCache) Configure(url, apiKey string, intervalMinutes int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	newURL := strings.TrimSpace(url)
	newKey := strings.TrimSpace(apiKey)
	if intervalMinutes < minRSSCheckMinutes {
		intervalMinutes = defaultRSSCheckMinutes
	}
	if intervalMinutes > maxRSSCheckMinutes {
		intervalMinutes = maxRSSCheckMinutes
	}
	newInterval := time.Duration(intervalMinutes) * time.Minute
	// Only invalidate the cached snapshot when the connection details actually
	// change. A purely cosmetic re-save must not trigger a re-fetch.
	if newURL != c.url || newKey != c.apiKey || newInterval != c.interval {
		c.items = nil
		c.lastFetch = time.Time{}
		c.lastErr = nil
	}
	c.url = newURL
	c.apiKey = newKey
	c.interval = newInterval
}

// feedURL builds the Newznab RSS query. Prowlarr and other indexers expose
// "latest releases" via t=1. We request the full extended feed once per
// interval, then match locally — the Radarr model.
func (c *rssFeedCache) feedURL() (string, error) {
	c.mu.Lock()
	raw := c.url
	key := c.apiKey
	c.mu.Unlock()
	if strings.TrimSpace(raw) == "" {
		return "", errors.New("indexer RSS URL is not configured")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("invalid indexer RSS URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("indexer RSS URL must be http or https")
	}
	q := u.Query()
	if q.Get("t") == "" {
		q.Set("t", "1")
	}
	q.Set("extended", "1")
	q.Set("limit", "200")
	if key != "" && q.Get("apikey") == "" {
		q.Set("apikey", key)
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// Stale reports whether the cached snapshot needs a refresh: the feed is
// unconfigured, empty, or older than the configured interval.
func (c *rssFeedCache) Stale() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.url == "" || c.lastFetch.IsZero() || time.Since(c.lastFetch) >= c.interval
}

// refreshIfStale refreshes the snapshot when it is stale. A failed fetch
// keeps the previous snapshot (if any) and records lastErr.
func (c *rssFeedCache) refreshIfStale(ctx context.Context) error {
	if !c.Stale() {
		return nil
	}
	return c.refresh(ctx)
}

func (c *rssFeedCache) refresh(ctx context.Context) error {
	feedURL, err := c.feedURL()
	if err != nil {
		c.mu.Lock()
		c.lastErr = err
		c.mu.Unlock()
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, feedURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/rss+xml, application/xml, text/xml")
	resp, err := c.client.Do(req)
	if err != nil {
		c.mu.Lock()
		c.lastErr = err
		c.mu.Unlock()
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		err := fmt.Errorf("indexer RSS returned status %d", resp.StatusCode)
		c.mu.Lock()
		c.lastErr = err
		c.mu.Unlock()
		return err
	}
	items, err := parseNewznabFeed(io.LimitReader(resp.Body, maxRSSFeedBytes+1))
	if err != nil {
		c.mu.Lock()
		c.lastErr = err
		c.mu.Unlock()
		return err
	}
	c.mu.Lock()
	c.items = items
	c.lastFetch = time.Now()
	c.lastErr = nil
	c.mu.Unlock()
	return nil
}

func parseNewznabFeed(r io.Reader) ([]newznabItem, error) {
	data, err := io.ReadAll(io.LimitReader(r, maxRSSFeedBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read indexer RSS: %w", err)
	}
	if int64(len(data)) > maxRSSFeedBytes {
		return nil, fmt.Errorf("indexer RSS exceeds %d bytes", maxRSSFeedBytes)
	}
	var feed struct {
		Channel struct {
			Items []newznabItem `xml:"item"`
		} `xml:"channel"`
	}
	if err := xml.Unmarshal(data, &feed); err != nil {
		return nil, fmt.Errorf("decode indexer RSS: %w", err)
	}
	if feed.Channel.Items == nil {
		var flat struct {
			Items []newznabItem `xml:"item"`
		}
		if err := xml.Unmarshal(data, &flat); err == nil && len(flat.Items) > 0 {
			return flat.Items, nil
		}
		return nil, nil
	}
	return feed.Channel.Items, nil
}

// releaseYearPattern extracts a 4-digit year from scene titles.
var releaseYearPattern = regexp.MustCompile(`\b(19|20)\d{2}\b`)

// releaseSepPattern replaces separators with spaces for tokenization.
var releaseSepPattern = regexp.MustCompile(`[._\-/()\[\]{}]`)

// releaseTagPattern strips common scene tags so only the core title remains.
var releaseTagPattern = regexp.MustCompile(`(?i)\b(2160p|1080p|720p|480p|4k|uhd|hdr|dolby|dolby.?vision|dv|hdr10|hdr10\+|ddp|dd|dts|dts-?hd|truehd|flac|aac|ac3|atmos|x264|x265|hevc|avc|av1|web-?dl|web-?rip|web|blu-?ray|brrip|bdrip|hdtv|dvdrip|remux|proper|repack|extended|imax|directors.?cut|unrated|multi|dual|10bit|8bit|10-?bit|h\.?264|h\.?265|amzn|amazon|nf|netflix|dsnp|disney|hbo|max|appletv|itunes|vudu|prime|red|group|post|subbed|dubbed|dual-?audio|hc|hdcam|cam|ts|telesync|telecine|screener|scr)\b.*`)

// releaseCleanPattern removes any remaining non-alphanumeric characters.
var releaseCleanPattern = regexp.MustCompile(`[^a-z0-9]+`)

func normalizeReleaseTitle(raw string) (title string, year int) {
	s := releaseSepPattern.ReplaceAllString(raw, " ")
	s = releaseTagPattern.ReplaceAllString(s, "")
	parts := strings.Fields(strings.ToLower(s))
	var kept []string
	for _, p := range parts {
		if m := releaseYearPattern.FindString(p); m != "" {
			if y, err := strconv.Atoi(m); err == nil {
				year = y
			}
			continue
		}
		if len(p) > 1 {
			kept = append(kept, p)
		}
	}
	cleaned := releaseCleanPattern.ReplaceAllString(strings.Join(kept, " "), " ")
	return strings.TrimSpace(cleaned), year

}

// Match returns true when the feed contains a release whose normalized
// title+year corresponds to the monitored media. When the monitored side has
// no year, a title-only match is accepted for titles of 4+ words to avoid
// false positives on short titles.
func (c *rssFeedCache) Match(item monitoredMedia) bool {
	c.mu.Lock()
	items := c.items
	c.mu.Unlock()
	if len(items) == 0 {
		return false
	}
	wantTitle, titleYear := normalizeReleaseTitle(item.Title)
	wantYear := item.Year
	if wantYear == 0 && titleYear != 0 {
		wantYear = int32(titleYear)
	}
	if wantTitle == "" {
		return false
	}
	for _, it := range items {
		gotTitle, gotYear := normalizeReleaseTitle(it.Title)
		if gotTitle == "" {
			continue
		}
		titleMatch := strings.Contains(gotTitle, wantTitle) || strings.Contains(wantTitle, gotTitle)
		if !titleMatch {
			continue
		}
		if wantYear != 0 && gotYear != 0 && int(wantYear) != gotYear {
			continue
		}
		// Require year+title match when year is known; title-only match only
		// for long, unambiguous titles (4+ tokens) to avoid false positives.
		if wantYear == 0 && len(strings.Fields(wantTitle)) < 2 {
			continue
		}
		return true
	}
	return false
}

// Validate performs a one-shot feed fetch and parse, returning a human-readable
// status message. Unlike refresh(), it always hits the network and never caches —
// it's used by the TestConnection handler.
func (c *rssFeedCache) Validate(ctx context.Context) (string, error) {
	feedURL, err := c.feedURL()
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, feedURL, nil)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Accept", "application/rss+xml, application/xml, text/xml")
	resp, err := c.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("connect to indexer: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("indexer returned HTTP %d", resp.StatusCode)
	}
	items, err := parseNewznabFeed(io.LimitReader(resp.Body, maxRSSFeedBytes+1))
	if err != nil {
		return "", fmt.Errorf("parse feed: %w", err)
	}
	return fmt.Sprintf("Indexer RSS feed OK: %d releases found", len(items)), nil
}
