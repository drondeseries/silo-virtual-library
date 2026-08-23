package release

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	defaultSchedulerInterval = 6 * time.Hour
)

// ShowProviderFunc provides a list of IMDb IDs currently tracked by the system.
type ShowProviderFunc func(ctx context.Context) ([]string, error)

// Scheduler manages periodic updates of show release schedules.
type Scheduler struct {
	store        *ReleaseStore
	client       *MetadataClient
	interval     time.Duration
	showProvider ShowProviderFunc
	stopCh       chan struct{}
	running      bool
	mu           sync.Mutex
}

// NewScheduler creates a new background release scheduler.
func NewScheduler(store *ReleaseStore, client *MetadataClient, interval time.Duration) *Scheduler {
	if interval <= 0 {
		interval = defaultSchedulerInterval
	}
	return &Scheduler{
		store:    store,
		client:   client,
		interval: interval,
		stopCh:   make(chan struct{}),
	}
}

// SetShowProvider configures a dynamic provider for tracked show IMDb IDs.
func (s *Scheduler) SetShowProvider(provider ShowProviderFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.showProvider = provider
}

// Start begins background scheduling. It runs an immediate check and then ticks at the configured interval.
func (s *Scheduler) Start(ctx context.Context, catalogPath string) {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return
	}
	s.running = true
	s.stopCh = make(chan struct{})
	s.mu.Unlock()

	// Initial sync pass
	s.runSync(ctx, catalogPath, false)

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.mu.Lock()
			s.running = false
			s.mu.Unlock()
			return
		case <-s.stopCh:
			s.mu.Lock()
			s.running = false
			s.mu.Unlock()
			return
		case <-ticker.C:
			s.runSync(ctx, catalogPath, false)
		}
	}
}

// Stop stops the scheduler.
func (s *Scheduler) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running {
		close(s.stopCh)
		s.running = false
	}
}

// RefreshAll triggers an immediate manual refresh of all tracked shows, regardless of status.
func (s *Scheduler) RefreshAll(ctx context.Context) error {
	return s.runSync(ctx, "", true)
}

// RefreshShow refreshes release metadata for a specific show.
func (s *Scheduler) RefreshShow(ctx context.Context, imdbID string) error {
	cleanID := normalizeID(imdbID)
	if cleanID == "" {
		return errors.New("imdb_id is required")
	}

	meta, err := s.client.FetchShowMetadata(ctx, cleanID)
	if err != nil {
		return fmt.Errorf("fetch metadata for %s: %w", cleanID, err)
	}

	schedule := &ShowSchedule{
		IMDBID:      cleanID,
		Status:      meta.Status,
		NextAirDate: meta.NextAirDate,
		Episodes:    meta.Episodes,
	}
	s.store.SetShow(cleanID, schedule)
	return nil
}

func (s *Scheduler) runSync(ctx context.Context, catalogPath string, forceAll bool) error {
	ids := s.collectShowIDs(ctx, catalogPath)
	if len(ids) == 0 {
		return nil
	}

	now := time.Now()
	var errs []error

	for _, id := range ids {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		cleanID := normalizeID(id)
		if cleanID == "" {
			continue
		}

		if !forceAll {
			// Check if we can skip updating this show
			if existing, ok := s.store.GetShow(cleanID); ok && existing != nil {
				isEnded := strings.EqualFold(existing.Status, "Ended")
				if isEnded && (existing.NextAirDate == nil || existing.NextAirDate.Before(now)) {
					continue
				}
				if existing.NextAirDate != nil && existing.NextAirDate.After(now) {
					continue
				}
			}
		}

		meta, err := s.client.FetchShowMetadata(ctx, cleanID)
		if err != nil {
			errs = append(errs, fmt.Errorf("fetch metadata for %s: %w", cleanID, err))
			continue
		}

		schedule := &ShowSchedule{
			IMDBID:      cleanID,
			Status:      meta.Status,
			NextAirDate: meta.NextAirDate,
			Episodes:    meta.Episodes,
		}
		s.store.SetShow(cleanID, schedule)
	}

	if len(errs) > 0 {
		return fmt.Errorf("encountered %d errors during release sync: %v", len(errs), errs[0])
	}
	return nil
}

func (s *Scheduler) collectShowIDs(ctx context.Context, catalogPath string) []string {
	seen := make(map[string]struct{})
	var result []string

	addID := func(id string) {
		clean := normalizeID(id)
		if clean != "" && strings.HasPrefix(clean, "tt") {
			if _, exists := seen[clean]; !exists {
				seen[clean] = struct{}{}
				result = append(result, clean)
			}
		}
	}

	// 1. From store
	for _, show := range s.store.GetAllShows() {
		if show != nil {
			addID(show.IMDBID)
		}
	}

	// 2. From provider
	s.mu.Lock()
	provider := s.showProvider
	s.mu.Unlock()
	if provider != nil {
		if providerIDs, err := provider(ctx); err == nil {
			for _, id := range providerIDs {
				addID(id)
			}
		}
	}

	// 3. From catalog/monitor file if exists
	if catalogPath != "" {
		if file, err := os.Open(catalogPath); err == nil {
			defer file.Close()
			var items []struct {
				Key       string `json:"key"`
				MediaType string `json:"media_type"`
				IMDbID    string `json:"imdb_id"`
				StreamID  string `json:"stream_id"`
			}
			if err := json.NewDecoder(io.LimitReader(file, 16<<20)).Decode(&items); err == nil {
				for _, item := range items {
					if item.MediaType == "series" {
						if item.IMDbID != "" {
							addID(item.IMDbID)
						} else if strings.HasPrefix(item.StreamID, "tt") {
							addID(item.StreamID)
						}
					}
				}
			}
		}
	}

	return result
}
