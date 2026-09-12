package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func altmountHistoryBody() []byte {
	payload := map[string]any{
		"history": map[string]any{
			"slots": []map[string]any{
				{
					"name":         "Movie.2024.2160p.WEB-DL.DDP5.1.HDR.x265-GRP",
					"status":       "Completed",
					"storage":      "/mnt/complete/Movie.2024.2160p.WEB-DL.DDP5.1.HDR.x265-GRP",
					"bytes":        int64(5_690_000_000),
					"completetime": time.Now().Unix(),
				},
				{
					"name":         "Bad.Movie.2024.1080p.WEB-DL.x264-BAD",
					"status":       "Failed",
					"bytes":        int64(120_000_000),
					"fail_message": "corrupt archive",
				},
				{
					// Pending/processing releases carry no completion state and
					// must not be treated as known-good or dead.
					"name":   "Pending.Movie.2024.720p.WEB-DL",
					"status": "Unknown",
				},
			},
		},
	}
	data, _ := json.Marshal(payload)
	return data
}

func TestParseAltmountHistoryClassifiesCompletedAndFailed(t *testing.T) {
	snapshot, err := parseAltmountHistory(strings.NewReader(string(altmountHistoryBody())), time.Now())
	if err != nil {
		t.Fatalf("parseAltmountHistory: %v", err)
	}
	if _, ok := snapshot.Completed[releaseNameKey("Movie.2024.2160p.WEB-DL.DDP5.1.HDR.x265-GRP")]; !ok {
		t.Fatal("expected completed release name key")
	}
	if _, ok := snapshot.Failed[releaseNameKey("Bad.Movie.2024.1080p.WEB-DL.x264-BAD")]; !ok {
		t.Fatal("expected failed release key")
	}
	if len(snapshot.Failed) != 1 {
		t.Fatalf("failed = %d, want 1", len(snapshot.Failed))
	}
	if _, ok := snapshot.Completed[releaseNameKey("Pending.Movie.2024.720p.WEB-DL")]; ok {
		t.Fatal("unknown-state release must not be recorded as completed")
	}
}

func TestAltmountStateClientRefreshAndClassify(t *testing.T) {
	var sawAPIKey string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/sabnzbd/api" {
			t.Errorf("path = %q, want /sabnzbd/api", r.URL.Path)
		}
		if mode := r.URL.Query().Get("mode"); mode != "history" {
			t.Errorf("mode = %q, want history", mode)
		}
		sawAPIKey = r.URL.Query().Get("apikey")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(altmountHistoryBody())
	}))
	defer srv.Close()

	dir := t.TempDir()
	indexFile := filepath.Join(dir, "altmount-state.json")
	client := newAltmountStateClient(srv.Client())
	client.Configure(srv.URL, "secret-key", 15)
	if err := client.ConfigureIndexFile(indexFile); err != nil {
		t.Fatalf("ConfigureIndexFile: %v", err)
	}
	if err := client.refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if sawAPIKey != "secret-key" {
		t.Fatalf("apikey = %q, want secret-key", sawAPIKey)
	}

	candidates := []StreamCandidate{
		{Title: "Movie.2024.2160p.WEB-DL.DDP5.1.HDR.x265-GRP", URL: "https://p.example/a.mkv", FileSize: 5_690_000_000},
		{Title: "Bad.Movie.2024.1080p.WEB-DL.x264-BAD", URL: "https://p.example/b.mkv"},
		{Title: "Pending.Movie.2024.720p.WEB-DL", URL: "https://p.example/c.mkv"},
	}
	client.ClassifyCandidates(candidates)
	if !candidates[0].SourceConfirmed {
		t.Fatal("completed candidate should be SourceConfirmed")
	}
	if !candidates[1].SourceFailed {
		t.Fatal("failed candidate should be SourceFailed")
	}
	if candidates[2].SourceConfirmed || candidates[2].SourceFailed {
		t.Fatal("unknown-state candidate must remain neutral")
	}

	// State must survive a restart through the index file.
	reloaded := newAltmountStateClient(srv.Client())
	reloaded.Configure(srv.URL, "secret-key", 15)
	if err := reloaded.ConfigureIndexFile(indexFile); err != nil {
		t.Fatalf("reload ConfigureIndexFile: %v", err)
	}
	reloadedCandidates := []StreamCandidate{{Title: "Movie.2024.2160p.WEB-DL.DDP5.1.HDR.x265-GRP"}}
	reloaded.ClassifyCandidates(reloadedCandidates)
	if !reloadedCandidates[0].SourceConfirmed {
		t.Fatal("persisted completed state should classify without a live refresh")
	}
}

func TestAltmountSABnzbdBaseNormalization(t *testing.T) {
	cases := map[string]string{
		"http://192.168.1.116:8083":             "http://192.168.1.116:8083/sabnzbd/api",
		"http://192.168.1.116:8083/":            "http://192.168.1.116:8083/sabnzbd/api",
		"http://192.168.1.116:8083/sabnzbd":     "http://192.168.1.116:8083/sabnzbd/api",
		"http://192.168.1.116:8083/sabnzbd/api": "http://192.168.1.116:8083/sabnzbd/api",
		"http://altmount:8080/SABnzbd/":         "http://altmount:8080/sabnzbd/api",
	}
	for in, want := range cases {
		if got := altmountSABnzbdBase(in); got != want {
			t.Errorf("altmountSABnzbdBase(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestAltmountStateCompletedWinsOverStaleFailed(t *testing.T) {
	now := time.Now()
	merged := mergeAltmountSnapshots(
		altmountStateSnapshot{Failed: map[string]altmountReleaseRecord{"samekey": {CompletedAt: now.Unix()}}},
		altmountStateSnapshot{Completed: map[string]altmountReleaseRecord{"samekey": {CompletedAt: now.Unix()}}},
		now,
	)
	if _, stillFailed := merged.Failed["samekey"]; stillFailed {
		t.Fatal("a release that completed must not stay classified failed")
	}
	if _, completed := merged.Completed["samekey"]; !completed {
		t.Fatal("completed record missing after merge")
	}
}

func TestAltmountStateNewerFailureOverridesCompletion(t *testing.T) {
	now := time.Now()
	merged := mergeAltmountSnapshots(
		altmountStateSnapshot{Completed: map[string]altmountReleaseRecord{"samekey": {CompletedAt: now.Add(-time.Hour).Unix()}}},
		altmountStateSnapshot{Failed: map[string]altmountReleaseRecord{"samekey": {CompletedAt: now.Unix()}}},
		now,
	)
	if _, stillCompleted := merged.Completed["samekey"]; stillCompleted {
		t.Fatal("a later failure must override an older completion")
	}
	if _, failed := merged.Failed["samekey"]; !failed {
		t.Fatal("newer failed record missing after merge")
	}
}

func TestAltmountStateRetentionPrunesOldRecords(t *testing.T) {
	now := time.Now()
	old := now.Add(-altmountStateRetention - time.Hour).Unix()
	snapshot := pruneAltmountSnapshot(altmountStateSnapshot{
		Completed: map[string]altmountReleaseRecord{"old": {CompletedAt: old}, "fresh": {CompletedAt: now.Unix()}},
	}, now)
	if _, ok := snapshot.Completed["old"]; ok {
		t.Fatal("expired record should be pruned")
	}
	if _, ok := snapshot.Completed["fresh"]; !ok {
		t.Fatal("fresh record should be retained")
	}
}

func TestAltmountBadgeMarksCandidateConfirmed(t *testing.T) {
	candidates := []StreamCandidate{
		{Name: "⚡ Cached · AltMount 🇪🇸 4K - Movie (2024) [2160p]", Title: "Movie.2024.2160p"},
		{Name: "AltMount - Other Movie (2024) [1080p]", Title: "Other.2024.1080p"},
	}
	markAltmountBadgeCandidates(candidates)
	if !candidates[0].SourceConfirmed {
		t.Fatal("cached badge should mark the candidate confirmed")
	}
	if candidates[1].SourceConfirmed {
		t.Fatal("unbadged candidate must not be confirmed")
	}
}
