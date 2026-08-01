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

type configuredVariantResolver interface {
	GetConfiguredVariants(string) []runtimehost.VirtualMediaVariant
}

// siloLibrary registers virtual media through Silo's authenticated RuntimeHost
// control plane. It intentionally has no database URL, driver, or SQL.
type siloLibrary struct {
	host            *runtimehost.Client
	movieLibraryID  int
	seriesLibraryID int
	resolver        streamResolver
}

func newSiloLibrary(host *runtimehost.Client, movieLibraryID, seriesLibraryID int, resolver streamResolver) (*siloLibrary, error) {
	if host == nil {
		return nil, errors.New("Silo host services are not ready")
	}
	if movieLibraryID <= 0 {
		movieLibraryID = 1
	}
	if seriesLibraryID <= 0 {
		seriesLibraryID = 2
	}
	return &siloLibrary{host: host, movieLibraryID: movieLibraryID, seriesLibraryID: seriesLibraryID, resolver: resolver}, nil
}

// validateLibraryIDs ensures the configured library IDs refer to real libraries
// on the Silo host.  On a fresh server the user must create libraries at
// Settings → Libraries before the virtual library plugin can register media.
func validateLibraryIDs(host *runtimehost.Client, movieID, seriesID int) error {
	if host == nil {
		return nil // skip validation when the host is unavailable (e.g. early Configure)
	}
	libs, err := host.ListLibraries(context.Background(), "")
	if err != nil {
		return fmt.Errorf("validate library configuration: list host libraries: %w", err)
	}
	if len(libs) == 0 {
		return errors.New(`No libraries found. Create a Movies and a Series library at Settings → Libraries first. Each library needs a folder path — point them at empty placeholder directories (e.g. /data/silo/movies and /data/silo/series). The plugin registers virtual media into these libraries without needing real files on disk.`)
	}
	moviesOK, seriesOK := false, false
	for _, lib := range libs {
		if lib == nil {
			continue
		}
		if lib.GetId() == strconv.Itoa(movieID) && (lib.GetMediaType() == "movie" || lib.GetMediaType() == "mixed") {
			moviesOK = true
		}
		if lib.GetId() == strconv.Itoa(seriesID) && (lib.GetMediaType() == "tv" || lib.GetMediaType() == "mixed") {
			seriesOK = true
		}
	}
	if !moviesOK {
		return fmt.Errorf("A Movies library with ID %d was not found on the host. Create one at Settings → Libraries first.", movieID)
	}
	if !seriesOK {
		return fmt.Errorf("A Series library with ID %d was not found on the host. Create one at Settings → Libraries first.", seriesID)
	}
	return nil
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

// movieVirtualURI returns the canonical virtual:// URI for a movie stream.
// Series playback sources belong to their episode rows; the Silo host rejects
// a series-level VirtualURI because there is no playable file at the container
// itself. Callers must guard on media type and attach per-episode URIs for
// series.
func movieVirtualURI(mediaType, streamID string) string {
	if mediaType != "movie" {
		return ""
	}
	return virtualPathPrefix + mediaType + "/" + strings.ReplaceAll(streamID, ":", "/")
}

func (l *siloLibrary) Register(ctx context.Context, item monitoredMedia) error {
	libraryID := l.movieLibraryID
	if item.MediaType == "series" {
		libraryID = l.seriesLibraryID
	}
	if item.MediaFolderID > 0 {
		libraryID = item.MediaFolderID
	}
	// Series playback sources belong to their episode rows.  The Silo host
	// rejects a series-level VirtualURI/Variants because there is no playable
	// file at the series container itself. Keep the canonical URI only for
	// movies and attach episode URIs below.
	canonicalStreamID := strings.ReplaceAll(item.StreamID, ":", "/")
	virtualURI := movieVirtualURI(item.MediaType, item.StreamID)
	episodes := make([]runtimehost.VirtualEpisode, 0, len(item.Episodes))
	for _, episode := range item.Episodes {
		if episode.Season <= 0 || episode.Episode <= 0 {
			continue
		}
		virtualEpURI := fmt.Sprintf("%sseries/%s/%d/%d", virtualPathPrefix, canonicalStreamID, episode.Season, episode.Episode)
		episodes = append(episodes, runtimehost.VirtualEpisode{
			SeasonNumber: episode.Season, EpisodeNumber: episode.Episode, Title: episode.Title, Overview: episode.Overview,
			AirDate: episode.Released, RuntimeMinutes: episode.Runtime, StillPath: episode.Thumbnail,
			// Profile variants are derived from local configuration only. Provider
			// streams are still resolved when playback starts.
			VirtualURI: virtualEpURI,
			Variants:   configuredVariants(l.resolver, virtualEpURI),
		})
	}
	req := runtimehost.VirtualMediaRequest{
		LibraryID: strconv.Itoa(libraryID), MediaType: item.MediaType, Title: item.Title, Year: int(item.Year),
		IMDbID: item.IMDbID, TMDBID: item.TMDBID, TVDBID: item.TVDBID, Overview: item.Overview, Genres: item.Genres,
		PosterPath: item.Poster, BackdropPath: item.Backdrop, VirtualURI: virtualURI, RuntimeMinutes: item.Runtime, Episodes: episodes,
		SourceKey: item.SourceKey,
	}
	if req.SourceKey == "" {
		req.SourceKey = "monitor"
	}
	if item.MediaType == "movie" {
		req.Variants = l.resolver.GetVariants(ctx, virtualURI)
		if len(req.Variants) == 0 {
			req.Variants = configuredVariants(l.resolver, virtualURI)
		}
	}
	_, err := l.host.UpsertVirtualMedia(ctx, req)
	if err != nil {
		return fmt.Errorf("register virtual media with Silo: %w", err)
	}
	return nil
}

func configuredVariants(resolver streamResolver, virtualURI string) []runtimehost.VirtualMediaVariant {
	if resolver == nil {
		return nil
	}
	if configured, ok := resolver.(configuredVariantResolver); ok {
		return configured.GetConfiguredVariants(virtualURI)
	}
	return nil
}

func (l *siloLibrary) Reconcile(ctx context.Context, sourceKey string, keepMediaIDs []string) error {
	_, err := l.host.ReconcileVirtualMedia(ctx, sourceKey, keepMediaIDs, []string{strconv.Itoa(l.movieLibraryID), strconv.Itoa(l.seriesLibraryID)})
	return err
}
