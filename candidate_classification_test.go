package main

import "testing"

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
