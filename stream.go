package main

import (
	"sort"
	"strings"
)

type StreamCandidate struct {
	URL           string
	Name          string
	Description   string
	Title         string
	BehaviorHints struct {
		VideoHash string `json:"videoHash"`
	}
	
	Resolution    string
	CodecVideo    string
	CodecAudio    string
	HDR           string
	SourceType    string
	OriginalIndex int
}

func parseStreamDetails(s *StreamCandidate) {
	fullText := strings.ToLower(s.Name + " " + s.Description + " " + s.Title)

	// Resolution
	if strings.Contains(fullText, "2160p") || strings.Contains(fullText, "4k") {
		s.Resolution = "2160p"
	} else if strings.Contains(fullText, "1080p") {
		s.Resolution = "1080p"
	} else if strings.Contains(fullText, "720p") {
		s.Resolution = "720p"
	} else if strings.Contains(fullText, "480p") || strings.Contains(fullText, "sd") {
		s.Resolution = "480p"
	}

	// Codec Video
	if strings.Contains(fullText, "hevc") || strings.Contains(fullText, "h265") || strings.Contains(fullText, "x265") {
		s.CodecVideo = "hevc"
	} else if strings.Contains(fullText, "h264") || strings.Contains(fullText, "x264") || strings.Contains(fullText, "avc") {
		s.CodecVideo = "h264"
	} else if strings.Contains(fullText, "av1") {
		s.CodecVideo = "av1"
	}

	// Codec Audio
	if strings.Contains(fullText, "atmos") {
		s.CodecAudio = "atmos"
	} else if strings.Contains(fullText, "truehd") || strings.Contains(fullText, "thd") {
		s.CodecAudio = "truehd"
	} else if strings.Contains(fullText, "dts-hd") || strings.Contains(fullText, "dtshd") {
		s.CodecAudio = "dts-hd"
	} else if strings.Contains(fullText, "dts") {
		s.CodecAudio = "dts"
	} else if strings.Contains(fullText, "eac3") || strings.Contains(fullText, "dd+") {
		s.CodecAudio = "eac3"
	} else if strings.Contains(fullText, "ac3") || strings.Contains(fullText, "dd") {
		s.CodecAudio = "ac3"
	} else if strings.Contains(fullText, "aac") {
		s.CodecAudio = "aac"
	}

	// HDR
	if strings.Contains(fullText, "hdr10+") {
		s.HDR = "hdr10+"
	} else if strings.Contains(fullText, "hdr10") {
		s.HDR = "hdr10"
	} else if strings.Contains(fullText, "dolby vision") || strings.Contains(fullText, "dv") {
		s.HDR = "dv"
	} else if strings.Contains(fullText, "hdr") {
		s.HDR = "hdr"
	}

	// Source Type
	if strings.Contains(fullText, "remux") {
		s.SourceType = "remux"
	} else if strings.Contains(fullText, "web-dl") || strings.Contains(fullText, "webdl") || strings.Contains(fullText, "web") {
		s.SourceType = "web-dl"
	} else if strings.Contains(fullText, "bluray") || strings.Contains(fullText, "blu-ray") || strings.Contains(fullText, "bdrip") {
		s.SourceType = "bluray"
	} else if strings.Contains(fullText, "hdtv") {
		s.SourceType = "hdtv"
	}
}

func resolutionScore(res string) int {
	switch res {
	case "2160p": return 4
	case "1080p": return 3
	case "720p": return 2
	case "480p": return 1
	}
	return 0
}

func sourceScore(src string) int {
	switch src {
	case "remux": return 4
	case "bluray": return 3
	case "web-dl": return 2
	case "hdtv": return 1
	}
	return 0
}

func matchProfile(c StreamCandidate, p QualityProfile) bool {
	fullText := c.Name + " " + c.Description + " " + c.Title
	if p.exclude != nil && p.exclude.MatchString(fullText) {
		return false
	}
	if p.include != nil && !p.include.MatchString(fullText) {
		return false
	}
	if p.Resolution != "" && c.Resolution != p.Resolution {
		return false
	}
	if p.CodecVideo != "" && c.CodecVideo != p.CodecVideo {
		return false
	}
	if p.CodecAudio != "" && c.CodecAudio != p.CodecAudio {
		return false
	}
	if p.HDR != "" && c.HDR != p.HDR {
		return false
	}
	return true
}

func sortCandidatesForProfile(candidates []StreamCandidate, p QualityProfile) {
	sort.SliceStable(candidates, func(i, j int) bool {
		c1, c2 := candidates[i], candidates[j]
		if r1, r2 := resolutionScore(c1.Resolution), resolutionScore(c2.Resolution); r1 != r2 {
			return r1 > r2
		}
		if s1, s2 := sourceScore(c1.SourceType), sourceScore(c2.SourceType); s1 != s2 {
			return s1 > s2
		}
		return c1.OriginalIndex < c2.OriginalIndex
	})
}
