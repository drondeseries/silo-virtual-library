package main

import (
	"fmt"
	"testing"
)

// mapClassifier is a deterministic candidateClassifier for ordering tests.
type mapClassifier struct {
	confirmed map[string]bool
	failed    map[string]bool
}

func (m mapClassifier) ClassifyCandidates(candidates []StreamCandidate) {
	for i := range candidates {
		if m.confirmed[candidates[i].Name] {
			candidates[i].SourceConfirmed = true
		}
		if m.failed[candidates[i].Name] {
			candidates[i].SourceFailed = true
		}
	}
}

func TestSelectCandidatesPrefersConfirmedAndDropsFailed(t *testing.T) {
	resolver := &manifestStreamResolver{}
	resolver.Configure(resolverConfig{})
	resolver.SetCandidateClassifier(mapClassifier{
		confirmed: map[string]bool{"confirmed-1080p": true},
		failed:    map[string]bool{"dead-720p": true},
	})
	candidates := []StreamCandidate{
		{Name: "dead-720p", Title: "720p", Resolution: "720p", OriginalIndex: 0, URL: "https://x/a.mkv"},
		{Name: "plain-2160p", Title: "2160p", Resolution: "2160p", OriginalIndex: 1, URL: "https://x/b.mkv"},
		{Name: "confirmed-1080p", Title: "1080p", Resolution: "1080p", OriginalIndex: 2, URL: "https://x/c.mkv"},
	}
	got := resolver.SelectCandidates("virtual://movie/tt1", candidates)
	if len(got) != 2 {
		t.Fatalf("candidates = %d, want 2 after dropping the known-dead release", len(got))
	}
	if got[0].Name != "confirmed-1080p" {
		t.Fatalf("first candidate = %q, want the completed release to lead", got[0].Name)
	}
	if got[1].Name != "plain-2160p" {
		t.Fatalf("second candidate = %q, want the neutral release retained", got[1].Name)
	}
}

func TestSelectCandidatesDropsFailedWhenAllAreDead(t *testing.T) {
	resolver := &manifestStreamResolver{}
	resolver.Configure(resolverConfig{})
	resolver.SetCandidateClassifier(mapClassifier{failed: map[string]bool{"dead": true}})
	got := resolver.SelectCandidates("virtual://movie/tt1", []StreamCandidate{
		{Name: "dead", Title: "2160p", Resolution: "2160p", URL: "https://x/a.mkv"},
	})
	if len(got) != 0 {
		t.Fatalf("candidates = %d, want 0 when every release is known-dead", len(got))
	}
}

func TestSelectCandidatesWithoutClassifierKeepsProviderOrder(t *testing.T) {
	resolver := &manifestStreamResolver{}
	resolver.Configure(resolverConfig{})
	candidates := []StreamCandidate{
		{Name: "first-720p", Title: "720p", Resolution: "720p", OriginalIndex: 0, URL: "https://x/a.mkv"},
		{Name: "second-2160p", Title: "2160p", Resolution: "2160p", OriginalIndex: 1, URL: "https://x/b.mkv"},
	}
	got := resolver.SelectCandidates("virtual://movie/tt1", candidates)
	if len(got) != 2 || got[0].Name != "second-2160p" {
		t.Fatalf("quality ranking should still apply without a classifier: %#v", got)
	}
}

// A multi-file torrent surfaces one provider candidate per file, with result
// IDs differing only by an appended file index. They must collapse to the
// highest-ranked variant so the version list holds one entry per release.
func TestSelectCandidatesDedupesSameReleaseVariants(t *testing.T) {
	resolver := &manifestStreamResolver{}
	resolver.Configure(resolverConfig{})
	release := "Show.S01E01.1080p.WEB-DL.x264-GRP"
	candidates := make([]StreamCandidate, 0, 5)
	for i := range 5 {
		candidates = append(candidates, StreamCandidate{
			Name:          "AltMount FHD",
			Title:         release,
			FileSize:      1_500_000_000,
			OriginalIndex: i,
			URL:           fmt.Sprintf("https://provider.example/play/cfffcc1ba480996d1c0323e4-%d", i),
		})
	}
	got := resolver.SelectCandidates("virtual://movie/tt1", candidates)
	if len(got) != 1 {
		t.Fatalf("candidates = %d, want 1 after collapsing per-file torrent variants", len(got))
	}
	if got[0].OriginalIndex != 0 {
		t.Fatalf("keeper OriginalIndex = %d, want 0 (highest-ranked variant)", got[0].OriginalIndex)
	}
}

// Same release name but a different size, a different release name, or a
// different quality profile describe distinct playable releases and must not be
// collapsed.
func TestSelectCandidatesKeepsDistinctReleases(t *testing.T) {
	resolver := &manifestStreamResolver{}
	resolver.Configure(resolverConfig{})
	candidates := []StreamCandidate{
		{Name: "same-name-diff-size", Title: "Movie.2024.1080p.WEB-DL.x264-GRP", FileSize: 5_000_000_000, OriginalIndex: 0, URL: "https://x/a.mkv"},
		{Name: "other-size", Title: "Movie.2024.1080p.WEB-DL.x264-GRP", FileSize: 6_000_000_000, OriginalIndex: 1, URL: "https://x/b.mkv"},
		{Name: "other-name", Title: "Movie.2024.2160p.WEB-DL.x264-OTHER", FileSize: 5_000_000_000, OriginalIndex: 2, URL: "https://x/c.mkv"},
		{Name: "profile-1080p", Title: "Movie.2024.WEB-DL.x264-GRP", Resolution: "1080p", FileSize: 5_000_000_000, OriginalIndex: 3, URL: "https://x/d.mkv"},
		{Name: "profile-2160p", Title: "Movie.2024.WEB-DL.x264-GRP", Resolution: "2160p", FileSize: 5_000_000_000, OriginalIndex: 4, URL: "https://x/e.mkv"},
	}
	got := resolver.SelectCandidates("virtual://movie/tt1", candidates)
	if len(got) != 5 {
		t.Fatalf("candidates = %d, want all 5 distinct releases retained", len(got))
	}
}

// When one variant of a collapse group is confirmed, the confirmed variant is
// the keeper even when it ranks below an unconfirmed sibling.
func TestSelectCandidatesDedupKeepsConfirmedVariant(t *testing.T) {
	resolver := &manifestStreamResolver{}
	resolver.Configure(resolverConfig{})
	resolver.SetCandidateClassifier(mapClassifier{confirmed: map[string]bool{"confirmed-variant": true}})
	candidates := []StreamCandidate{
		{Name: "plain-variant", Title: "Movie.2024.1080p.WEB-DL.x264-GRP", FileSize: 5_000_000_000, OriginalIndex: 0, URL: "https://x/plain.mkv"},
		{Name: "confirmed-variant", Title: "Movie.2024.1080p.WEB-DL.x264-GRP", FileSize: 5_000_000_000, OriginalIndex: 1, URL: "https://x/confirmed.mkv"},
	}
	got := resolver.SelectCandidates("virtual://movie/tt1", candidates)
	if len(got) != 1 {
		t.Fatalf("candidates = %d, want 1 collapsed variant", len(got))
	}
	if got[0].Name != "confirmed-variant" || !got[0].SourceConfirmed {
		t.Fatalf("keeper = %q (confirmed %t), want the confirmed variant", got[0].Name, got[0].SourceConfirmed)
	}
}

// A failed variant is removed before deduplication so it cannot shadow a live
// duplicate of the same release.
func TestSelectCandidatesDedupDropsFailedBeforeLiveDuplicate(t *testing.T) {
	resolver := &manifestStreamResolver{}
	resolver.Configure(resolverConfig{})
	resolver.SetCandidateClassifier(mapClassifier{failed: map[string]bool{"dead-variant": true}})
	candidates := []StreamCandidate{
		{Name: "dead-variant", Title: "Movie.2024.1080p.WEB-DL.x264-GRP", FileSize: 5_000_000_000, OriginalIndex: 0, URL: "https://x/dead.mkv"},
		{Name: "live-variant", Title: "Movie.2024.1080p.WEB-DL.x264-GRP", FileSize: 5_000_000_000, OriginalIndex: 1, URL: "https://x/live.mkv"},
	}
	got := resolver.SelectCandidates("virtual://movie/tt1", candidates)
	if len(got) != 1 || got[0].Name != "live-variant" {
		t.Fatalf("candidates = %#v, want only the live duplicate retained", got)
	}
}
