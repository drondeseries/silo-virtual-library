package main

import "testing"

func TestCanonicalVirtualURIOnlyExistsForMovies(t *testing.T) {
	if got := canonicalVirtualURI("movie", "tt123:4"); got != "virtual://movie/tt123/4" {
		t.Fatalf("movie URI = %q, want virtual://movie/tt123/4", got)
	}
	if got := canonicalVirtualURI("series", "tt123"); got != "" {
		t.Fatalf("series URI = %q, want empty series-level URI", got)
	}
}
