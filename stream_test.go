package main

import "testing"

// A release marker is not a language: "MULTI" in a release name must not be
// declared as an audio language on the candidate. The server's virtual-track
// merge filters it with the ISO language tagger, but the declared list should
// already be clean so Jellyfin-protocol clients and rankers see real
// languages only.
func TestParseStreamMetadataAudioLanguagesExcludeReleaseMarkers(t *testing.T) {
	s := &StreamCandidate{
		Name:        "Movie.2023.2160p.MULTI.TRUEHD.Atmos.WebLD-RGB",
		Description: "Multi audio release",
		Title:       "Movie 2023 MULTI eng fre 2160p",
	}
	parseStreamMetadata(s)

	got := map[string]bool{}
	for _, lang := range s.AudioLanguages {
		got[lang] = true
	}
	if got["MULTI"] {
		t.Fatalf("release marker MULTI leaked into AudioLanguages: %v", s.AudioLanguages)
	}
	for _, lang := range s.AudioLanguages {
		switch lang {
		case "ENG", "FRE":
			// expected
		default:
			t.Errorf("unexpected language token %q in AudioLanguages: %v", lang, s.AudioLanguages)
		}
	}
	if len(s.AudioLanguages) == 0 {
		t.Fatal("expected at least the ENG/FRE tokens from the title, got none")
	}
}

func TestParseStreamMetadataAudioLanguagesDeduplicates(t *testing.T) {
	s := &StreamCandidate{
		Name:  "Movie.2160p.ENG.ENG.eng.Multi",
		Title: "Movie ENG FRE",
	}
	parseStreamMetadata(s)

	counts := map[string]int{}
	for _, lang := range s.AudioLanguages {
		counts[lang]++
	}
	for lang, count := range counts {
		if count > 1 {
			t.Errorf("language %q appeared %d times, want once: %v", lang, count, s.AudioLanguages)
		}
	}
	if counts["ENG"] != 1 {
		t.Errorf("ENG appeared %d times, want 1: %v", counts["ENG"], s.AudioLanguages)
	}
}
