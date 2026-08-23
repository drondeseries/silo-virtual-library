package release

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// ShowSchedule represents the cached schedule and episode release states for a show.
type ShowSchedule struct {
	IMDBID      string                 `json:"imdb_id"`
	Title       string                 `json:"title,omitempty"`
	Status      string                 `json:"status"`
	NextAirDate *time.Time             `json:"next_air_date,omitempty"`
	Episodes    map[string]EpisodeInfo `json:"episodes"`
	UpdatedAt   time.Time              `json:"updated_at"`
}

// ReleaseStore is a thread-safe in-memory cache for show and movie release schedules.
type ReleaseStore struct {
	mu     sync.RWMutex
	shows  map[string]*ShowSchedule
	movies map[string]time.Time
}

// NewReleaseStore creates a new ReleaseStore instance.
func NewReleaseStore() *ReleaseStore {
	return &ReleaseStore{
		shows:  make(map[string]*ShowSchedule),
		movies: make(map[string]time.Time),
	}
}

func normalizeID(id string) string {
	clean := strings.TrimSpace(strings.ToLower(id))
	// If id contains colons like "series:tt1234567:1:2" or "tt1234567:1:2", take the imdb part
	if strings.HasPrefix(clean, "series:") || strings.HasPrefix(clean, "movie:") {
		parts := strings.Split(clean, ":")
		if len(parts) > 1 {
			clean = parts[1]
		}
	} else if strings.Contains(clean, ":") {
		parts := strings.Split(clean, ":")
		clean = parts[0]
	}
	return clean
}

// GetShow retrieves the schedule for the given IMDb ID.
func (s *ReleaseStore) GetShow(imdbID string) (*ShowSchedule, bool) {
	key := normalizeID(imdbID)
	if key == "" {
		return nil, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	show, ok := s.shows[key]
	if !ok || show == nil {
		return nil, false
	}
	// Return a shallow copy with cloned episodes map to prevent race conditions
	cloned := *show
	cloned.Episodes = make(map[string]EpisodeInfo, len(show.Episodes))
	for k, v := range show.Episodes {
		cloned.Episodes[k] = v
	}
	return &cloned, true
}

// SetShow updates or inserts a show schedule.
func (s *ReleaseStore) SetShow(imdbID string, schedule *ShowSchedule) {
	key := normalizeID(imdbID)
	if key == "" || schedule == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	scheduleCopy := *schedule
	scheduleCopy.IMDBID = key
	scheduleCopy.UpdatedAt = time.Now().UTC()
	s.shows[key] = &scheduleCopy
}

// SetShowIfAbsent inserts a show schedule only when no entry exists yet,
// returning the effective schedule afterwards. The scheduler is the
// authoritative writer of show statuses; this lets secondary sources seed a
// first guess without clobbering authoritative TVmaze state.
func (s *ReleaseStore) SetShowIfAbsent(imdbID string, schedule *ShowSchedule) (*ShowSchedule, bool) {
	key := normalizeID(imdbID)
	if key == "" || schedule == nil {
		return nil, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.shows[key]; ok && existing != nil {
		cloned := *existing
		cloned.Episodes = make(map[string]EpisodeInfo, len(existing.Episodes))
		for k, v := range existing.Episodes {
			cloned.Episodes[k] = v
		}
		return &cloned, false
	}
	scheduleCopy := *schedule
	scheduleCopy.IMDBID = key
	scheduleCopy.UpdatedAt = time.Now().UTC()
	s.shows[key] = &scheduleCopy
	inserted := scheduleCopy
	return &inserted, true
}

// SetMovie updates or inserts a movie release date.
func (s *ReleaseStore) SetMovie(imdbID string, releaseDate time.Time) {
	key := normalizeID(imdbID)
	if key == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.movies[key] = releaseDate
}

// GetMovie retrieves a movie release date.
func (s *ReleaseStore) GetMovie(imdbID string) (time.Time, bool) {
	key := normalizeID(imdbID)
	if key == "" {
		return time.Time{}, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	d, ok := s.movies[key]
	return d, ok
}

// IsReleased checks whether an item (show, episode, or movie) has been released.
// If the air date is unknown or not yet tracked, it defaults to (true, nil)
// to prevent false-positive playback blockage.
// If the air date is strictly in the future, it returns (false, &airDate).
func (s *ReleaseStore) IsReleased(itemType string, imdbID string, season int, episode int) (bool, *time.Time) {
	key := normalizeID(imdbID)
	if key == "" {
		return true, nil
	}

	normType := strings.ToLower(strings.TrimSpace(itemType))
	now := time.Now()

	if normType == "movie" {
		s.mu.RLock()
		releaseDate, ok := s.movies[key]
		s.mu.RUnlock()
		if !ok || releaseDate.IsZero() {
			return true, nil
		}
		if releaseDate.After(now) {
			dateCopy := releaseDate
			return false, &dateCopy
		}
		dateCopy := releaseDate
		return true, &dateCopy
	}

	// Series / Episode lookup
	s.mu.RLock()
	show, ok := s.shows[key]
	s.mu.RUnlock()

	if !ok || show == nil {
		return true, nil
	}

	if season > 0 && episode > 0 {
		epKey := fmt.Sprintf("%d:%d", season, episode)
		ep, exists := show.Episodes[epKey]
		if !exists || ep.AirDate.IsZero() {
			return true, nil
		}
		if ep.AirDate.After(now) {
			dateCopy := ep.AirDate
			return false, &dateCopy
		}
		dateCopy := ep.AirDate
		return true, &dateCopy
	}

	// General series query without specific episode
	if show.NextAirDate != nil && show.NextAirDate.After(now) {
		return true, show.NextAirDate
	}
	return true, nil
}

// GetAllShows returns a slice of all tracked show schedules. Episodes maps
// are deep-copied so callers can never race writers through shared state.
func (s *ReleaseStore) GetAllShows() []*ShowSchedule {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]*ShowSchedule, 0, len(s.shows))
	for _, show := range s.shows {
		if show != nil {
			cloned := *show
			cloned.Episodes = make(map[string]EpisodeInfo, len(show.Episodes))
			for k, v := range show.Episodes {
				cloned.Episodes[k] = v
			}
			result = append(result, &cloned)
		}
	}
	return result
}

// GetScheduleSummary produces a structured summary of the store for administrative routes.
func (s *ReleaseStore) GetScheduleSummary() map[string]any {
	shows := s.GetAllShows()
	return map[string]any{
		"count":        len(shows),
		"shows":        shows,
		"generated_at": time.Now().UTC(),
	}
}
