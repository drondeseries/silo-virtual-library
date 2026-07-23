package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/Silo-Server/silo-plugin-sdk/pkg/pluginsdk/runtimehost"
)

type virtualMediaRegistrar interface {
	Register(context.Context, monitoredMedia) error
}

// siloLibrary registers virtual media through Silo's authenticated RuntimeHost
// control plane. It intentionally has no database URL, driver, or SQL.
type siloLibrary struct {
	host            *runtimehost.Client
	movieLibraryID  int
	seriesLibraryID int
	resolver        aioStreamsResolver
}

func newSiloLibrary(host *runtimehost.Client, movieLibraryID, seriesLibraryID int, resolver aioStreamsResolver) (*siloLibrary, error) {
	if host == nil {
		return nil, errors.New("Silo host services are not ready")
	}
	if movieLibraryID <= 0 || seriesLibraryID <= 0 {
		return nil, errors.New("movie_library_id and series_library_id are required")
	}
	return &siloLibrary{host: host, movieLibraryID: movieLibraryID, seriesLibraryID: seriesLibraryID, resolver: resolver}, nil
}

func configuredFolderID(value any) (int, error) {
	switch v := value.(type) {
	case float64:
		if v > 0 && v == float64(int(v)) {
			return int(v), nil
		}
	case string:
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil && n > 0 {
			return n, nil
		}
	case nil:
		return 0, nil
	}
	return 0, errors.New("library ID must be a positive integer")
}

func (l *siloLibrary) Register(ctx context.Context, item monitoredMedia) error {
	libraryID := l.movieLibraryID
	if item.MediaType == "series" {
		libraryID = l.seriesLibraryID
	}
	if item.MediaFolderID > 0 {
		libraryID = item.MediaFolderID
	}
	virtualURI := virtualPathPrefix + item.MediaType + "/" + strings.ReplaceAll(item.StreamID, ":", "/")
	episodes := make([]runtimehost.VirtualEpisode, 0, len(item.Episodes))
	for _, episode := range item.Episodes {
		if episode.Season <= 0 || episode.Episode <= 0 {
			continue
		}
		virtualEpURI := fmt.Sprintf("%sseries/%s/%d/%d", virtualPathPrefix, item.StreamID, episode.Season, episode.Episode)
		episodes = append(episodes, runtimehost.VirtualEpisode{
			SeasonNumber: episode.Season, EpisodeNumber: episode.Episode, Title: episode.Title, Overview: episode.Overview,
			AirDate: episode.Released, RuntimeMinutes: episode.Runtime, StillPath: episode.Thumbnail,
			VirtualURI: virtualEpURI,
			Variants:   l.resolver.GetVariants(ctx, virtualEpURI),
		})
	}
	req := runtimehost.VirtualMediaRequest{
		LibraryID: strconv.Itoa(libraryID), MediaType: item.MediaType, Title: item.Title, Year: int(item.Year),
		IMDbID: item.IMDbID, TMDBID: item.TMDBID, TVDBID: item.TVDBID, Overview: item.Overview, Genres: item.Genres,
		PosterPath: item.Poster, BackdropPath: item.Backdrop, VirtualURI: virtualURI, RuntimeMinutes: item.Runtime, Episodes: episodes,
	}
	if item.MediaType == "movie" {
		req.Variants = l.resolver.GetVariants(ctx, virtualURI)
	}

	_, err := l.host.UpsertVirtualMedia(ctx, req)
	if err != nil {
		return fmt.Errorf("register virtual media with Silo: %w", err)
	}
	return nil
}
