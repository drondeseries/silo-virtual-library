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
	minSchedulerInterval     = 30 * time.Minute
	maxSchedulerInterval     = 10080 * time.Minute
	// refreshWorkerCount bounds concurrent TVmaze lookups so a manual
	// refresh cannot stampede the provider regardless of library size.
	refreshWorkerCount = 4
	// refreshOverallDeadline caps every sync pass well under the host's
	// two-minute scheduled-task gRPC deadline (handshake.go).
	refreshOverallDeadline = 90 * time.Second
)

// ShowProviderFunc provides a list of IMDb IDs currently tracked by the system.
type ShowProviderFunc func(ctx context.Context) ([]string, error)

// Scheduler manages periodic updates of show release schedules.
type Scheduler struct {
	store        *ReleaseStore
	client       *MetadataClient
	interval     time.Duration
	showProvider ShowProviderFunc
	catalogPath  string

	syncMu  sync.Mutex // singleflight: one sync pass at a time
	mu      sync.Mutex // guards mutable fields below
	stopCh  chan struct{}
	running bool
}

// NewScheduler creates a new background release scheduler. Any positive
// interval is honored; configuration-supplied values are clamped by
// SetInterval.
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

func clampInterval(interval time.Duration) time.Duration {
	if interval < minSchedulerInterval {
		return defaultSchedulerInterval
	}
	if interval > maxSchedulerInterval {
		return maxSchedulerInterval
	}
	return interval
}

// SetShowProvider configures a dynamic provider for tracked show IMDb IDs.
func (s *Scheduler) SetShowProvider(provider ShowProviderFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.showProvider = provider
}

// SetCatalogPath points the scheduler at the monitor queue file resolved by
// Configure. It is safe to call before Start and between ticks.
func (s *Scheduler) SetCatalogPath(path string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.catalogPath = strings.TrimSpace(path)
}

// SetInterval adjusts the tick cadence. Values apply the next time the
// scheduler starts; a running scheduler keeps its current cadence until
// restarted by the host.
func (s *Scheduler) SetInterval(interval time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if interval > 0 {
		s.interval = clampInterval(interval)
	}
}

// Start begins background scheduling. It runs an immediate check and then
// ticks at the configured interval.
func (s *Scheduler) Start(ctx context.Context, catalogPath string) {
	if catalogPath != "" {
		s.SetCatalogPath(catalogPath)
	}
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return
	}
	s.running = true
	stop := make(chan struct{})
	s.stopCh = stop
	interval := s.interval
	s.mu.Unlock()

	// Initial sync pass
	s.runSync(ctx, false)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.mu.Lock()
			s.running = false
			s.mu.Unlock()
			return
		case <-stop:
			s.mu.Lock()
			s.running = false
			s.mu.Unlock()
			return
		case <-ticker.C:
			s.runSync(ctx, false)
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

// RefreshAll triggers an immediate manual refresh of all tracked shows,
// regardless of status.
func (s *Scheduler) RefreshAll(ctx context.Context) error {
	return s.runSync(ctx, true)
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

	s.store.SetShow(cleanID, &ShowSchedule{
		IMDBID:      cleanID,
		Title:       meta.Title,
		Status:      meta.Status,
		NextAirDate: meta.NextAirDate,
		Episodes:    meta.Episodes,
	})
	return nil
}

// runSync refreshes every tracked show that is due. Passes are serialized
// (manual refresh cannot overlap the periodic tick), bounded by a worker
// pool, and capped by an overall deadline so neither the admin endpoint nor
// the scheduled task can hang past the host deadline.
func (s *Scheduler) runSync(ctx context.Context, forceAll bool) error {
	s.syncMu.Lock()
	defer s.syncMu.Unlock()

	syncCtx, cancel := context.WithTimeout(ctx, refreshOverallDeadline)
	defer cancel()

	ids := s.collectShowIDs(syncCtx)
	if len(ids) == 0 {
		return nil
	}

	due := make([]string, 0, len(ids))
	now := time.Now()
	for _, id := range ids {
		if syncCtx.Err() != nil {
			break
		}
		cleanID := normalizeID(id)
		if cleanID == "" || (!forceAll && !s.showDue(cleanID, now)) {
			continue
		}
		due = append(due, cleanID)
	}
	if len(due) == 0 {
		return nil
	}

	jobs := make(chan string)
	errCh := make(chan error, len(due))
	var wg sync.WaitGroup
	workers := refreshWorkerCount
	if workers > len(due) {
		workers = len(due)
	}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range jobs {
				if err := s.fetchAndStore(syncCtx, id); err != nil {
					errCh <- err
				}
			}
		}()
	}
	for _, id := range due {
		if syncCtx.Err() != nil {
			break
		}
		jobs <- id
	}
	close(jobs)
	wg.Wait()
	close(errCh)

	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}
	if syncCtx.Err() != nil && !errors.Is(syncCtx.Err(), context.Canceled) || ctx.Err() != nil {
		errs = append(errs, fmt.Errorf("release sync stopped early: %w", syncCtx.Err()))
	}
	return errors.Join(errs...)
}

// showDue reports whether a show needs refetching: ended shows whose last
// known air date has passed are skipped, and shows with a known future air
// date are skipped until it arrives. Unknown statuses refresh normally.
func (s *Scheduler) showDue(id string, now time.Time) bool {
	existing, ok := s.store.GetShow(id)
	if !ok || existing == nil {
		return true
	}
	isEnded := strings.EqualFold(existing.Status, "Ended")
	if isEnded && (existing.NextAirDate == nil || existing.NextAirDate.Before(now)) {
		return false
	}
	if existing.NextAirDate != nil && existing.NextAirDate.After(now) {
		return false
	}
	return true
}

func (s *Scheduler) fetchAndStore(ctx context.Context, cleanID string) error {
	meta, err := s.client.FetchShowMetadata(ctx, cleanID)
	if err != nil {
		return fmt.Errorf("fetch metadata for %s: %w", cleanID, err)
	}
	s.store.SetShow(cleanID, &ShowSchedule{
		IMDBID:      cleanID,
		Title:       meta.Title,
		Status:      meta.Status,
		NextAirDate: meta.NextAirDate,
		Episodes:    meta.Episodes,
	})
	return nil
}

func (s *Scheduler) collectShowIDs(ctx context.Context) []string {
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
	path := s.catalogPath
	s.mu.Unlock()
	if provider != nil {
		if providerIDs, err := provider(ctx); err == nil {
			for _, id := range providerIDs {
				addID(id)
			}
		}
	}

	// 3. From the configured monitor queue file, if reachable
	if path != "" {
		if file, err := os.Open(path); err == nil {
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
