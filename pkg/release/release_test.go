package release

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestMetadataClient_FetchShowMetadata(t *testing.T) {
	futureDate := time.Now().Add(7 * 24 * time.Hour).UTC().Format(time.RFC3339)
	pastDate := time.Now().Add(-7 * 24 * time.Hour).UTC().Format(time.RFC3339)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/lookup/shows":
			imdb := r.URL.Query().Get("imdb")
			if imdb == "tt9999999" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if imdb == "tt1234567" {
				resp := tvMazeShowResponse{
					ID:        100,
					Name:      "House of Dragons",
					Status:    "Running",
					Premiered: "2022-08-21",
				}
				resp.Embedded.Episodes = []tvMazeEpisodeResponse{
					{
						ID:       1001,
						Name:     "Episode 1",
						Season:   1,
						Number:   1,
						Airstamp: pastDate,
					},
					{
						ID:       1002,
						Name:     "Episode 2",
						Season:   1,
						Number:   2,
						Airstamp: futureDate,
					},
				}
				_ = json.NewEncoder(w).Encode(resp)
				return
			}
			if imdb == "tt7654321" {
				// Ended show
				resp := tvMazeShowResponse{
					ID:        200,
					Name:      "Succession",
					Status:    "Ended",
					Premiered: "2018-06-03",
				}
				resp.Embedded.Episodes = []tvMazeEpisodeResponse{
					{
						ID:       2001,
						Name:     "Final Episode",
						Season:   4,
						Number:   10,
						Airstamp: pastDate,
					},
				}
				_ = json.NewEncoder(w).Encode(resp)
				return
			}
			w.WriteHeader(http.StatusBadRequest)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client := NewMetadataClient(server.Client())
	client.SetBaseURL(server.URL)

	// Test 1: Ongoing show with future episode
	meta, err := client.FetchShowMetadata(context.Background(), "tt1234567")
	if err != nil {
		t.Fatalf("FetchShowMetadata failed: %v", err)
	}
	if meta.Status != "Running" {
		t.Errorf("Status = %q, want 'Running'", meta.Status)
	}
	if len(meta.Episodes) != 2 {
		t.Errorf("Episodes count = %d, want 2", len(meta.Episodes))
	}
	if meta.NextAirDate == nil {
		t.Fatalf("NextAirDate is nil, expected future date")
	}
	if ep1, ok := meta.Episodes["1:1"]; !ok || ep1.AirDate.After(time.Now()) {
		t.Errorf("Episode 1:1 should be in the past, got %v", ep1)
	}
	if ep2, ok := meta.Episodes["1:2"]; !ok || ep2.AirDate.Before(time.Now()) {
		t.Errorf("Episode 1:2 should be in the future, got %v", ep2)
	}

	// Test 2: Ended show
	endedMeta, err := client.FetchShowMetadata(context.Background(), "tt7654321")
	if err != nil {
		t.Fatalf("FetchShowMetadata ended show failed: %v", err)
	}
	if endedMeta.Status != "Ended" {
		t.Errorf("Status = %q, want 'Ended'", endedMeta.Status)
	}
	if endedMeta.NextAirDate != nil {
		t.Errorf("NextAirDate = %v, want nil for ended show with all past episodes", endedMeta.NextAirDate)
	}

	// Test 3: Show not found
	_, err = client.FetchShowMetadata(context.Background(), "tt9999999")
	if err == nil {
		t.Errorf("expected error for non-existent show, got nil")
	}
}

func TestReleaseStore_IsReleased(t *testing.T) {
	store := NewReleaseStore()
	past := time.Now().Add(-24 * time.Hour)
	future := time.Now().Add(24 * time.Hour)

	store.SetShow("tt1234567", &ShowSchedule{
		IMDBID: "tt1234567",
		Status: "Running",
		Episodes: map[string]EpisodeInfo{
			"1:1": {Season: 1, Episode: 1, AirDate: past, Title: "Aired Episode"},
			"1:2": {Season: 1, Episode: 2, AirDate: future, Title: "Future Episode"},
			"1:3": {Season: 1, Episode: 3, AirDate: time.Time{}, Title: "Zero AirDate"},
		},
	})

	store.SetMovie("tt5555555", future)
	store.SetMovie("tt6666666", past)

	tests := []struct {
		name         string
		itemType     string
		imdbID       string
		season       int
		episode      int
		wantReleased bool
		hasAirDate   bool
	}{
		{
			name:         "past episode released",
			itemType:     "series",
			imdbID:       "tt1234567",
			season:       1,
			episode:      1,
			wantReleased: true,
			hasAirDate:   true,
		},
		{
			name:         "future episode unreleased",
			itemType:     "series",
			imdbID:       "tt1234567",
			season:       1,
			episode:      2,
			wantReleased: false,
			hasAirDate:   true,
		},
		{
			name:         "zero airdate fallback released",
			itemType:     "series",
			imdbID:       "tt1234567",
			season:       1,
			episode:      3,
			wantReleased: true,
			hasAirDate:   false,
		},
		{
			name:         "untracked episode fallback released",
			itemType:     "series",
			imdbID:       "tt1234567",
			season:       2,
			episode:      1,
			wantReleased: true,
			hasAirDate:   false,
		},
		{
			name:         "untracked show fallback released",
			itemType:     "series",
			imdbID:       "tt0000000",
			season:       1,
			episode:      1,
			wantReleased: true,
			hasAirDate:   false,
		},
		{
			name:         "future movie unreleased",
			itemType:     "movie",
			imdbID:       "tt5555555",
			wantReleased: false,
			hasAirDate:   true,
		},
		{
			name:         "past movie released",
			itemType:     "movie",
			imdbID:       "tt6666666",
			wantReleased: true,
			hasAirDate:   true,
		},
		{
			name:         "untracked movie fallback released",
			itemType:     "movie",
			imdbID:       "tt0000001",
			wantReleased: true,
			hasAirDate:   false,
		},
		{
			name:         "prefixed compound ID series:tt1234567:1:2",
			itemType:     "series",
			imdbID:       "series:tt1234567:1:2",
			season:       1,
			episode:      2,
			wantReleased: false,
			hasAirDate:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			released, airDate := store.IsReleased(tc.itemType, tc.imdbID, tc.season, tc.episode)
			if released != tc.wantReleased {
				t.Errorf("IsReleased(%q, %q, %d, %d) = %v, want %v", tc.itemType, tc.imdbID, tc.season, tc.episode, released, tc.wantReleased)
			}
			if tc.hasAirDate && airDate == nil {
				t.Errorf("expected non-nil airDate")
			}
			if !tc.hasAirDate && airDate != nil {
				t.Errorf("expected nil airDate, got %v", airDate)
			}
		})
	}
}

func TestReleaseStore_Concurrency(t *testing.T) {
	store := NewReleaseStore()
	const numGoroutines = 50
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 3)

	// Writers
	for i := 0; i < numGoroutines; i++ {
		id := fmt.Sprintf("tt%07d", i)
		go func(imdbID string) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				store.SetShow(imdbID, &ShowSchedule{
					IMDBID: imdbID,
					Status: "Running",
					Episodes: map[string]EpisodeInfo{
						"1:1": {Season: 1, Episode: 1, AirDate: time.Now().Add(time.Duration(j) * time.Hour)},
					},
				})
				store.SetMovie(imdbID, time.Now().Add(time.Duration(j)*time.Hour))
			}
		}(id)
	}

	// Readers (GetShow / GetMovie)
	for i := 0; i < numGoroutines; i++ {
		id := fmt.Sprintf("tt%07d", i)
		go func(imdbID string) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				_, _ = store.GetShow(imdbID)
				_, _ = store.GetMovie(imdbID)
				_ = store.GetAllShows()
			}
		}(id)
	}

	// Checkers (IsReleased)
	for i := 0; i < numGoroutines; i++ {
		id := fmt.Sprintf("tt%07d", i)
		go func(imdbID string) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				_, _ = store.IsReleased("series", imdbID, 1, 1)
				_, _ = store.IsReleased("movie", imdbID, 0, 0)
			}
		}(id)
	}

	wg.Wait()
}

func TestScheduler_RefreshAndSkipEnded(t *testing.T) {
	fetchCount := 0
	var mu sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		fetchCount++
		mu.Unlock()

		imdb := r.URL.Query().Get("imdb")
		status := "Running"
		if imdb == "tt7777777" {
			status = "Ended"
		}

		resp := tvMazeShowResponse{
			ID:     1,
			Name:   "Test Show",
			Status: status,
		}
		resp.Embedded.Episodes = []tvMazeEpisodeResponse{
			{
				ID:       1,
				Season:   1,
				Number:   1,
				Airstamp: time.Now().Add(-24 * time.Hour).Format(time.RFC3339),
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewMetadataClient(server.Client())
	client.SetBaseURL(server.URL)

	store := NewReleaseStore()
	scheduler := NewScheduler(store, client, 100*time.Millisecond)
	scheduler.SetShowProvider(func(ctx context.Context) ([]string, error) {
		return []string{"tt1111111", "tt7777777"}, nil
	})

	// Initial sync pass
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go scheduler.Start(ctx, "")
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	initialFetches := fetchCount
	mu.Unlock()

	if initialFetches < 2 {
		t.Fatalf("expected at least 2 fetches during initial sync, got %d", initialFetches)
	}

	// Wait for one or two ticks. The ended show (tt7777777) should be skipped!
	time.Sleep(250 * time.Millisecond)
	scheduler.Stop()

	// Verify show schedules in store
	endedShow, ok := store.GetShow("tt7777777")
	if !ok || endedShow.Status != "Ended" {
		t.Fatalf("expected ended show in store with status Ended, got %v", endedShow)
	}

	// Trigger RefreshAll - should update all shows including Ended
	mu.Lock()
	beforeRefresh := fetchCount
	mu.Unlock()

	if err := scheduler.RefreshAll(context.Background()); err != nil {
		t.Fatalf("RefreshAll failed: %v", err)
	}

	mu.Lock()
	afterRefresh := fetchCount
	mu.Unlock()

	if afterRefresh-beforeRefresh < 2 {
		t.Errorf("RefreshAll should have fetched all shows, before=%d, after=%d", beforeRefresh, afterRefresh)
	}
}
