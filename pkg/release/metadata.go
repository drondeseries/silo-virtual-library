package release

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	defaultTVMazeBaseURL = "https://api.tvmaze.com"
	maxMetadataBytes     = 8 << 20
)

// EpisodeInfo contains release schedule info for an individual episode.
type EpisodeInfo struct {
	Season  int       `json:"season"`
	Episode int       `json:"episode"`
	AirDate time.Time `json:"air_date"`
	Title   string    `json:"title,omitempty"`
}

// ShowMetadata contains release schedule and status info for a TV series.
type ShowMetadata struct {
	IMDBID      string                 `json:"imdb_id"`
	Status      string                 `json:"status"`
	NextAirDate *time.Time             `json:"next_air_date,omitempty"`
	Episodes    map[string]EpisodeInfo `json:"episodes"`
}

// MovieMetadata contains release information for a movie.
type MovieMetadata struct {
	IMDBID      string     `json:"imdb_id"`
	ReleaseDate *time.Time `json:"release_date,omitempty"`
}

// MetadataClient fetches show and episode metadata from external providers.
type MetadataClient struct {
	client  *http.Client
	baseURL string
}

// NewMetadataClient creates a new client with the given HTTP client or a default client.
func NewMetadataClient(client *http.Client) *MetadataClient {
	if client == nil {
		client = &http.Client{
			Timeout: 20 * time.Second,
		}
	}
	return &MetadataClient{
		client:  client,
		baseURL: defaultTVMazeBaseURL,
	}
}

// SetBaseURL overrides the base URL (primarily for testing with mock servers).
func (c *MetadataClient) SetBaseURL(baseURL string) {
	c.baseURL = strings.TrimRight(baseURL, "/")
}

type tvMazeEpisodeResponse struct {
	ID       int    `json:"id"`
	Name     string `json:"name"`
	Season   int    `json:"season"`
	Number   int    `json:"number"`
	Airdate  string `json:"airdate"`
	Airtime  string `json:"airtime"`
	Airstamp string `json:"airstamp"`
}

type tvMazeShowResponse struct {
	ID        int    `json:"id"`
	Name      string `json:"name"`
	Status    string `json:"status"`
	Premiered string `json:"premiered"`
	Embedded  struct {
		Episodes []tvMazeEpisodeResponse `json:"episodes"`
	} `json:"_embedded"`
}

// FetchShowMetadata fetches show schedule information from TVmaze using the IMDb ID.
func (c *MetadataClient) FetchShowMetadata(ctx context.Context, imdbID string) (*ShowMetadata, error) {
	cleanID := strings.TrimSpace(imdbID)
	if cleanID == "" {
		return nil, errors.New("imdb_id is required")
	}

	endpoint := fmt.Sprintf("%s/lookup/shows?imdb=%s&embed=episodes", c.baseURL, url.QueryEscape(cleanID))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("create TVmaze request: %w", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute TVmaze request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("show not found on TVmaze for imdb_id %q", cleanID)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("TVmaze returned HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxMetadataBytes))
	if err != nil {
		return nil, fmt.Errorf("read TVmaze response: %w", err)
	}

	var show tvMazeShowResponse
	if err := json.Unmarshal(body, &show); err != nil {
		return nil, fmt.Errorf("decode TVmaze show: %w", err)
	}

	episodes := show.Embedded.Episodes
	// Fallback if _embedded was omitted during redirect
	if len(episodes) == 0 && show.ID > 0 {
		episodes, err = c.fetchEpisodes(ctx, show.ID)
		if err != nil {
			return nil, err
		}
	}

	now := time.Now()
	episodeMap := make(map[string]EpisodeInfo, len(episodes))
	var nextAirDate *time.Time

	for _, ep := range episodes {
		if ep.Season <= 0 || ep.Number <= 0 {
			continue
		}
		airDate := parseAirDate(ep.Airstamp, ep.Airdate, ep.Airtime)
		info := EpisodeInfo{
			Season:  ep.Season,
			Episode: ep.Number,
			AirDate: airDate,
			Title:   strings.TrimSpace(ep.Name),
		}
		key := fmt.Sprintf("%d:%d", ep.Season, ep.Number)
		episodeMap[key] = info

		if !airDate.IsZero() && airDate.After(now) {
			if nextAirDate == nil || airDate.Before(*nextAirDate) {
				airCopy := airDate
				nextAirDate = &airCopy
			}
		}
	}

	return &ShowMetadata{
		IMDBID:      cleanID,
		Status:      strings.TrimSpace(show.Status),
		NextAirDate: nextAirDate,
		Episodes:    episodeMap,
	}, nil
}

func (c *MetadataClient) fetchEpisodes(ctx context.Context, showID int) ([]tvMazeEpisodeResponse, error) {
	endpoint := fmt.Sprintf("%s/shows/%d/episodes", c.baseURL, showID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("create TVmaze episodes request: %w", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute TVmaze episodes request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("TVmaze episodes returned HTTP %d", resp.StatusCode)
	}

	var episodes []tvMazeEpisodeResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxMetadataBytes)).Decode(&episodes); err != nil {
		return nil, fmt.Errorf("decode TVmaze episodes: %w", err)
	}
	return episodes, nil
}

// parseAirDate attempts to parse airstamp (RFC3339) or airdate + airtime.
func parseAirDate(airstamp, airdate, airtime string) time.Time {
	if airstamp != "" {
		if t, err := time.Parse(time.RFC3339, airstamp); err == nil {
			return t
		}
		if t, err := time.Parse("2006-01-02T15:04:05-0700", airstamp); err == nil {
			return t
		}
	}
	if airdate != "" {
		if airtime != "" {
			if t, err := time.Parse("2006-01-02 15:04", airdate+" "+airtime); err == nil {
				return t
			}
		}
		if t, err := time.Parse("2006-01-02", airdate); err == nil {
			return t
		}
	}
	return time.Time{}
}
