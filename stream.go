package main

import (
	"regexp"
	"sort"
	"strconv"
	"strings"
)

var streamSizePattern = regexp.MustCompile(`(?i)\b\d+(?:\.\d+)?\s*(?:TB|GB|MB)\b`)
var languagePattern = regexp.MustCompile(`(?i)\b(?:eng|en|fra|fre|fr|deu|ger|de|ita|es|spa|jpn|kor|zho|chi|por|rus|ara|multi)\b`)
var (
	dolbyVisionPattern = regexp.MustCompile(`(?i)(?:\bdolby[ ._-]*vision\b|\bdv\b)`)
	atmosPattern       = regexp.MustCompile(`(?i)\batmos\b`)
	trueHDPattern      = regexp.MustCompile(`(?i)(?:\btrue[ ._-]*hd\b|\bthd\b)`)
	dtsHDPattern       = regexp.MustCompile(`(?i)\bdts[ ._-]*hd\b`)
	dtsPattern         = regexp.MustCompile(`(?i)\bdts\b`)
	eac3Pattern        = regexp.MustCompile(`(?i)(?:\be[ ._-]*ac[ ._-]*3\b|\bdd\+)`)
	ac3Pattern         = regexp.MustCompile(`(?i)(?:\bac[ ._-]*3\b|\bdd\b)`)
	aacPattern         = regexp.MustCompile(`(?i)\baac\b`)
)

type StreamCandidate struct {
	URL           string
	Name          string
	Description   string
	Title         string
	BehaviorHints struct {
		VideoHash string `json:"videoHash"`
	}

	Resolution        string
	CodecVideo        string
	CodecAudio        string
	HDR               string
	SourceType        string
	FileSize          int64
	Container         string
	AudioLanguages    []string
	SubtitleLanguages []string
	OriginalIndex     int
}

func parseStreamDetails(s *StreamCandidate) {
	metadataText := strings.ToLower(s.Name + " " + s.Description + " " + s.Title)
	fullText := metadataText + " " + strings.ToLower(s.URL)

	// Resolution
	// Prefer explicit provider metadata. URL tokens can contain unrelated
	// strings such as "4k" in an opaque identifier.
	resolutionText := metadataText
	if !hasResolutionMarker(resolutionText) {
		resolutionText = fullText
	}
	if strings.Contains(resolutionText, "2160p") || strings.Contains(resolutionText, "4k") {
		s.Resolution = "2160p"
	} else if strings.Contains(resolutionText, "1080p") {
		s.Resolution = "1080p"
	} else if strings.Contains(resolutionText, "720p") {
		s.Resolution = "720p"
	} else if strings.Contains(resolutionText, "480p") || strings.Contains(resolutionText, "sd") {
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
	if atmosPattern.MatchString(fullText) {
		s.CodecAudio = "atmos"
	} else if trueHDPattern.MatchString(fullText) {
		s.CodecAudio = "truehd"
	} else if dtsHDPattern.MatchString(fullText) {
		s.CodecAudio = "dts-hd"
	} else if dtsPattern.MatchString(fullText) {
		s.CodecAudio = "dts"
	} else if eac3Pattern.MatchString(fullText) {
		s.CodecAudio = "eac3"
	} else if ac3Pattern.MatchString(fullText) {
		s.CodecAudio = "ac3"
	} else if aacPattern.MatchString(fullText) {
		s.CodecAudio = "aac"
	}

	// HDR
	if strings.Contains(fullText, "hdr10+") {
		s.HDR = "hdr10+"
	} else if strings.Contains(fullText, "hdr10") {
		s.HDR = "hdr10"
	} else if dolbyVisionPattern.MatchString(fullText) {
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

func hasResolutionMarker(text string) bool {
	return strings.Contains(text, "2160p") || strings.Contains(text, "4k") ||
		strings.Contains(text, "1080p") || strings.Contains(text, "720p") ||
		strings.Contains(text, "480p")
}

func streamSize(s StreamCandidate) string {
	return streamSizePattern.FindString(s.Name + " " + s.Description + " " + s.Title)
}

func parseStreamMetadata(s *StreamCandidate) {
	text := s.Name + " " + s.Description + " " + s.Title
	if size := streamSizePattern.FindString(text); size != "" {
		parts := strings.Fields(strings.ToUpper(size))
		if len(parts) == 2 {
			if value, err := strconv.ParseFloat(parts[0], 64); err == nil {
				multiplier := float64(1)
				switch parts[1] {
				case "TB":
					multiplier = 1e12
				case "GB":
					multiplier = 1e9
				case "MB":
					multiplier = 1e6
				}
				s.FileSize = int64(value * multiplier)
			}
		}
	}
	seen := map[string]bool{}
	for _, match := range languagePattern.FindAllString(strings.ToLower(text), -1) {
		match = strings.ToUpper(match)
		if !seen[match] {
			seen[match] = true
			s.AudioLanguages = append(s.AudioLanguages, match)
		}
	}
	lowerURL := strings.ToLower(s.URL)
	for _, ext := range []string{".mkv", ".mp4", ".webm", ".avi", ".mov"} {
		if strings.Contains(lowerURL, ext) {
			s.Container = strings.TrimPrefix(ext, ".")
			break
		}
	}
}

func resolutionScore(res string) int {
	switch res {
	case "2160p":
		return 4
	case "1080p":
		return 3
	case "720p":
		return 2
	case "480p":
		return 1
	}
	return 0
}

func sourceScore(src string) int {
	switch src {
	case "remux":
		return 4
	case "bluray":
		return 3
	case "web-dl":
		return 2
	case "hdtv":
		return 1
	}
	return 0
}

func matchProfile(c StreamCandidate, p QualityProfile) bool {
	fullText := c.Name + " " + c.Description + " " + c.Title + " " + c.URL
	if p.exclude != nil && p.exclude.MatchString(fullText) {
		return false
	}
	if p.include != nil && !p.include.MatchString(fullText) {
		return false
	}
	if p.Resolution != "" && normalizeResolution(c.Resolution) != normalizeResolution(p.Resolution) {
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

func normalizeResolution(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "4k", "uhd", "2160":
		return "2160p"
	case "2k", "1440":
		return "1440p"
	case "1080":
		return "1080p"
	case "720":
		return "720p"
	case "480":
		return "480p"
	default:
		return strings.ToLower(strings.TrimSpace(value))
	}
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
