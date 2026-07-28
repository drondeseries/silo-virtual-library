package main

import (
	"context"
	"fmt"
	"strings"

	pb "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
)

const virtualStreamProviderID = "com.drondeseries.silo-virtual-library"

// virtualStreamProvider exposes the provider-neutral SDK contract. The host
// asks for candidates at playback time; provider URLs are deliberately not
// persisted in the catalog.
type virtualStreamProvider struct{ resolver *manifestStreamResolver }

func (s *virtualStreamProvider) ResolveVirtualStream(ctx context.Context, req *pb.ResolveVirtualStreamRequest) (*pb.ResolveVirtualStreamResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("resolve request is required")
	}
	path, err := virtualPathForRequest(req)
	if err != nil {
		return nil, err
	}
	candidates, _, _, err := s.resolver.GetCandidates(ctx, path)
	if err != nil {
		return &pb.ResolveVirtualStreamResponse{Result: &pb.VirtualStreamResult{
			ProviderId:   virtualStreamProviderID,
			Availability: &pb.VirtualStreamAvailability{State: pb.VirtualStreamAvailabilityState_VIRTUAL_STREAM_AVAILABILITY_STATE_UNAVAILABLE, Message: err.Error()},
			Error:        &pb.VirtualStreamError{Code: pb.VirtualStreamErrorCode_VIRTUAL_STREAM_ERROR_CODE_PROVIDER_FAILURE, Message: err.Error(), Retryable: true},
		}}, nil
	}
	result := &pb.VirtualStreamResult{ResultId: path, ProviderId: virtualStreamProviderID, Availability: &pb.VirtualStreamAvailability{State: pb.VirtualStreamAvailabilityState_VIRTUAL_STREAM_AVAILABILITY_STATE_AVAILABLE}}
	for rank, candidate := range candidates {
		result.Candidates = append(result.Candidates, &pb.VirtualStreamCandidate{
			CandidateId: candidateVariantID(candidate), ProviderId: virtualStreamProviderID, TemporaryUri: candidate.URL,
			Rank: int32(rank + 1), Resolution: &pb.VirtualStreamResolution{Label: candidate.Resolution},
			VideoCodec: candidate.CodecVideo, AudioCodec: candidate.CodecAudio,
			Hdr:           &pb.VirtualStreamHDR{IsHdr: candidate.HDR != "", Format: candidate.HDR, HasDolbyVision: strings.EqualFold(candidate.HDR, "dv")},
			FileSizeBytes: candidate.FileSize, Container: candidate.Container,
			AudioLanguages: candidate.AudioLanguages, SubtitleLanguages: candidate.SubtitleLanguages,
			Availability: &pb.VirtualStreamAvailability{State: pb.VirtualStreamAvailabilityState_VIRTUAL_STREAM_AVAILABILITY_STATE_AVAILABLE},
		})
	}
	if len(result.Candidates) == 0 {
		result.Availability = &pb.VirtualStreamAvailability{State: pb.VirtualStreamAvailabilityState_VIRTUAL_STREAM_AVAILABILITY_STATE_UNAVAILABLE, Message: "provider returned no streams"}
		result.Error = &pb.VirtualStreamError{Code: pb.VirtualStreamErrorCode_VIRTUAL_STREAM_ERROR_CODE_NOT_FOUND, Message: "provider returned no streams", Retryable: true}
	}
	return &pb.ResolveVirtualStreamResponse{Result: result}, nil
}

func virtualPathForRequest(req *pb.ResolveVirtualStreamRequest) (string, error) {
	ids := req.GetExternalIds()
	id := strings.TrimSpace(ids["imdb"])
	if id == "" {
		if tvdb := strings.TrimSpace(ids["tvdb"]); tvdb != "" {
			id = "tvdb:" + tvdb
		} else if tmdb := strings.TrimSpace(ids["tmdb"]); tmdb != "" {
			id = "tmdb:" + tmdb
		}
	}
	if id == "" {
		return "", fmt.Errorf("an IMDb, TVDB, or TMDB external ID is required")
	}
	typ := strings.ToLower(strings.TrimSpace(req.GetMediaType()))
	if typ == "episode" {
		if req.GetSeasonNumber() <= 0 || req.GetEpisodeNumber() <= 0 {
			return "", fmt.Errorf("season and episode are required")
		}
		typ = "series"
		return fmt.Sprintf("virtual://series/%s/%d/%d", id, req.GetSeasonNumber(), req.GetEpisodeNumber()), nil
	}
	if typ != "movie" && typ != "series" {
		return "", fmt.Errorf("unsupported media type %q", typ)
	}
	return "virtual://" + typ + "/" + id, nil
}

var _ pb.VirtualStreamProviderServer = (*virtualStreamProvider)(nil)
