package main

import (
	"fmt"
	"testing"
)

// mapClassifier is a deterministic candidateClassifier for ordering tests. It
// mimics the production classifiers: guids attaches the stable release GUID
// each Prowlarr-confirmed candidate would carry, so dedup can key on it.
type mapClassifier struct {
	confirmed map[string]bool
	failed    map[string]bool
	guids     map[string]string
}

func (m mapClassifier) ClassifyCandidates(candidates []StreamCandidate) {
	for i := range candidates {
		if m.confirmed[candidates[i].Name] {
			candidates[i].SourceConfirmed = true
		}
		if m.failed[candidates[i].Name] {
			candidates[i].SourceFailed = true
		}
		if guid := m.guids[candidates[i].Name]; guid != "" {
			candidates[i].SourceGUID = guid
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

// Same release name but a different size or a different release name describe
// distinct playable releases and must not be collapsed. The quality profile is
// deliberately not part of the fallback identity (see the collapse test below).
func TestSelectCandidatesKeepsDistinctReleases(t *testing.T) {
	resolver := &manifestStreamResolver{}
	resolver.Configure(resolverConfig{})
	candidates := []StreamCandidate{
		{Name: "same-name-diff-size", Title: "Movie.2024.1080p.WEB-DL.x264-GRP", FileSize: 5_000_000_000, OriginalIndex: 0, URL: "https://x/a.mkv"},
		{Name: "other-size", Title: "Movie.2024.1080p.WEB-DL.x264-GRP", FileSize: 6_000_000_000, OriginalIndex: 1, URL: "https://x/b.mkv"},
		{Name: "other-name", Title: "Movie.2024.2160p.WEB-DL.x264-OTHER", FileSize: 5_000_000_000, OriginalIndex: 2, URL: "https://x/c.mkv"},
	}
	got := resolver.SelectCandidates("virtual://movie/tt1", candidates)
	if len(got) != 3 {
		t.Fatalf("candidates = %d, want all 3 distinct releases retained", len(got))
	}
}

// One release re-offered per file can parse a different resolution/codec/HDR
// from its differing result ID, but a shared release name and size are enough
// to call it one release: the fallback identity ignores the quality profile.
func TestSelectCandidatesDedupesSameNameAndSizeAcrossProfile(t *testing.T) {
	resolver := &manifestStreamResolver{}
	resolver.Configure(resolverConfig{})
	candidates := []StreamCandidate{
		{Name: "profile-1080p", Title: "Movie.2024.WEB-DL.x264-GRP", Resolution: "1080p", CodecAudio: "aac", FileSize: 5_000_000_000, OriginalIndex: 0, URL: "https://x/d.mkv"},
		{Name: "profile-1080p-ac3", Title: "Movie.2024.WEB-DL.x264-GRP", Resolution: "1080p", CodecAudio: "ac3", FileSize: 5_000_000_000, OriginalIndex: 1, URL: "https://x/e.mkv"},
		{Name: "profile-2160p", Title: "Movie.2024.WEB-DL.x264-GRP", Resolution: "2160p", HDR: "hdr", FileSize: 5_000_000_000, OriginalIndex: 2, URL: "https://x/f.mkv"},
	}
	got := resolver.SelectCandidates("virtual://movie/tt1", candidates)
	if len(got) != 1 {
		t.Fatalf("candidates = %d, want 1 collapsed release despite differing profiles", len(got))
	}
}

// behaviorHints.filename names a single file inside a multi-file release, so
// per-file variants of one torrent must still collapse on their shared
// release-title line and size.
func TestSelectCandidatesDedupesPerFileFilenames(t *testing.T) {
	resolver := &manifestStreamResolver{}
	resolver.Configure(resolverConfig{})
	release := "Show.S01E01.1080p.WEB-DL.x264-GRP"
	candidates := []StreamCandidate{
		{Name: "AltMount FHD", Title: release, FileSize: 1_500_000_000, OriginalIndex: 0, URL: "https://provider.example/play/episode"},
		{Name: "AltMount FHD", Title: release, FileSize: 1_500_000_000, OriginalIndex: 1, URL: "https://provider.example/play/sample"},
	}
	candidates[0].BehaviorHints.Filename = release + ".mkv"
	candidates[1].BehaviorHints.Filename = release + ".sample.mkv"
	got := resolver.SelectCandidates("virtual://movie/tt1", candidates)
	if len(got) != 1 {
		t.Fatalf("candidates = %d, want 1 after collapsing per-file filenames", len(got))
	}
}

// When only the provider result ID is available, the trailing per-file index
// (`<hash>` vs `<hash>-43`) is not part of the release identity.
func TestSelectCandidatesDedupesPerFileResultIDs(t *testing.T) {
	resolver := &manifestStreamResolver{}
	resolver.Configure(resolverConfig{})
	hash := "cfffcc1ba480996d1c0323e4"
	candidates := []StreamCandidate{
		{FileSize: 1_500_000_000, OriginalIndex: 0, URL: "https://provider.example/play/" + hash},
		{FileSize: 1_500_000_000, OriginalIndex: 1, URL: "https://provider.example/play/" + hash + "-43"},
	}
	got := resolver.SelectCandidates("virtual://movie/tt1", candidates)
	if len(got) != 1 {
		t.Fatalf("candidates = %d, want 1 after collapsing per-file result IDs", len(got))
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

// A non-empty VideoHash is the strongest identity: candidates that share it
// collapse even when their display names and sizes differ.
func TestSelectCandidatesDedupesByVideoHash(t *testing.T) {
	resolver := &manifestStreamResolver{}
	resolver.Configure(resolverConfig{})
	candidates := []StreamCandidate{
		{Name: "variant-a", Title: "Movie.2024.1080p.WEB-DL.x264-GRP", FileSize: 5_000_000_000, OriginalIndex: 0, URL: "https://x/a.mkv"},
		{Name: "variant-b", Title: "Movie.2024.2160p.BluRay.x265-OTHER", FileSize: 9_000_000_000, OriginalIndex: 1, URL: "https://x/b.mkv"},
	}
	candidates[0].BehaviorHints.VideoHash = "ABCDEF0123456789"
	candidates[1].BehaviorHints.VideoHash = "abcdef0123456789"
	got := resolver.SelectCandidates("virtual://movie/tt1", candidates)
	if len(got) != 1 {
		t.Fatalf("candidates = %d, want 1 collapsed by video hash", len(got))
	}
}

// Two candidates the classifier tied to the same indexed release GUID collapse
// even with different names and sizes: per the source of truth, a shared GUID
// is enough to call them one release.
func TestSelectCandidatesDedupesByReleaseGUID(t *testing.T) {
	resolver := &manifestStreamResolver{}
	resolver.Configure(resolverConfig{})
	resolver.SetCandidateClassifier(mapClassifier{confirmed: map[string]bool{
		"variant-a": true,
		"variant-b": true,
	}, guids: map[string]string{
		"variant-a": "guid-abc123",
		"variant-b": "guid-abc123",
	}})
	candidates := []StreamCandidate{
		{Name: "variant-a", Title: "Movie.2024.1080p.WEB-DL.x264-GRP", FileSize: 5_000_000_000, Resolution: "1080p", OriginalIndex: 0, URL: "https://x/a.mkv"},
		{Name: "variant-b", Title: "Movie.2024.2160p.BluRay.x265-OTHER", FileSize: 9_000_000_000, Resolution: "2160p", OriginalIndex: 1, URL: "https://x/b.mkv"},
	}
	got := resolver.SelectCandidates("virtual://movie/tt1", candidates)
	if len(got) != 1 {
		t.Fatalf("candidates = %d, want 1 collapsed by release GUID despite differing size/profile", len(got))
	}
}

// Distinct GUIDs describe distinct releases and must not collapse, even when
// names, sizes, and profiles are otherwise identical.
func TestSelectCandidatesKeepsDistinctReleaseGUIDs(t *testing.T) {
	resolver := &manifestStreamResolver{}
	resolver.Configure(resolverConfig{})
	resolver.SetCandidateClassifier(mapClassifier{guids: map[string]string{
		"variant-a": "guid-abc123",
		"variant-b": "guid-def456",
	}})
	candidates := []StreamCandidate{
		{Name: "variant-a", Title: "Movie.2024.1080p.WEB-DL.x264-GRP", FileSize: 5_000_000_000, OriginalIndex: 0, URL: "https://x/a.mkv"},
		{Name: "variant-b", Title: "Movie.2024.1080p.WEB-DL.x264-GRP", FileSize: 5_000_000_000, OriginalIndex: 1, URL: "https://x/b.mkv"},
	}
	got := resolver.SelectCandidates("virtual://movie/tt1", candidates)
	if len(got) != 2 {
		t.Fatalf("candidates = %d, want distinct GUIDs kept separate", len(got))
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
