package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

const (
	maxQualityProfiles       = 10
	maxCustomFormats         = 50
	maxProfileLabelBytes     = 128
	maxProfileRegexBytes     = 1024
	maxProfileAttributeBytes = 64
	maxPreferredOrder        = 10000
)

type CustomFormat struct {
	Name   string `json:"name"`
	Regex  string `json:"regex"`
	Score  int    `json:"score"`
	Reject bool   `json:"reject"`

	match *regexp.Regexp
}

type QualityProfile struct {
	Label          string `json:"label"`
	Resolution     string `json:"resolution"`
	IncludeRegex   string `json:"include_regex"`
	ExcludeRegex   string `json:"exclude_regex"`
	PreferredOrder int    `json:"preferred_order"`
	CodecVideo     string `json:"codec_video"`
	CodecAudio     string `json:"codec_audio"`
	HDR            string `json:"hdr"`
	ExcludeHDR     string `json:"exclude_hdr"`

	include *regexp.Regexp
	exclude *regexp.Regexp
}

type QualityConfig struct {
	Preset              string           `json:"quality_preset"`
	EnableProfiles      bool             `json:"enable_quality_profiles"`
	Profiles            []QualityProfile `json:"quality_profiles"`
	CustomFormats       []CustomFormat   `json:"custom_formats"`
	FallbackToAnyStream bool             `json:"fallback_to_any_stream"`
}

const defaultQualityPreset = "custom"

// qualityPresetProfiles keeps the common choices understandable in the admin
// form while leaving regex-level control available through Custom.
func qualityPresetProfiles(preset string) []QualityProfile {
	switch strings.ToLower(strings.TrimSpace(preset)) {
	case "balanced":
		return []QualityProfile{{Label: "1080p", Resolution: "1080p", PreferredOrder: 1}}
	case "4k-hdr":
		return []QualityProfile{{Label: "4K HDR", Resolution: "2160p", HDR: "hdr", PreferredOrder: 1}, {Label: "1080p", Resolution: "1080p", PreferredOrder: 2}}
	case "4k-dolby-vision":
		return []QualityProfile{{Label: "4K Dolby Vision", Resolution: "2160p", HDR: "dv", PreferredOrder: 1}, {Label: "1080p", Resolution: "1080p", PreferredOrder: 2}}
	case "no-dolby-vision":
		return []QualityProfile{{Label: "4K HDR10", Resolution: "2160p", ExcludeHDR: "dv", ExcludeRegex: `(?i)(dolby[ ._-]*vision|\bdv\b|dovi)`, PreferredOrder: 1}, {Label: "1080p", Resolution: "1080p", ExcludeHDR: "dv", ExcludeRegex: `(?i)(dolby[ ._-]*vision|\bdv\b|dovi)`, PreferredOrder: 2}}
	case "no-hdr":
		return []QualityProfile{{Label: "4K SDR", Resolution: "2160p", ExcludeHDR: "*", ExcludeRegex: `(?i)(hdr|dolby[ ._-]*vision|\bdv\b|dovi|hlg)`, PreferredOrder: 1}, {Label: "1080p SDR", Resolution: "1080p", ExcludeHDR: "*", ExcludeRegex: `(?i)(hdr|dolby[ ._-]*vision|\bdv\b|dovi|hlg)`, PreferredOrder: 2}}
	case "compatibility":
		return []QualityProfile{{Label: "1080p Compatible", Resolution: "1080p", CodecVideo: "h264", CodecAudio: "aac", PreferredOrder: 1}, {Label: "720p Compatible", Resolution: "720p", CodecVideo: "h264", CodecAudio: "aac", PreferredOrder: 2}}
	case "anime":
		return []QualityProfile{{Label: "Anime 1080p", Resolution: "1080p", IncludeRegex: `(?i)(anime|web-dl|web)`, PreferredOrder: 1}, {Label: "Anime 720p", Resolution: "720p", IncludeRegex: `(?i)(anime|web-dl|web)`, PreferredOrder: 2}}
	default:
		return nil
	}
}

func (q *QualityConfig) ApplyPreset() {
	if profiles := qualityPresetProfiles(q.Preset); len(profiles) > 0 {
		q.Profiles = profiles
	}
}

// decodeQualityProfiles accepts the typed array declared by the plugin
// manifest. Keeping one representation prevents configuration drift between
// the UI, JSON Schema validation, and the runtime.
func decodeQualityProfiles(raw any) ([]QualityProfile, error) {
	if raw == nil {
		return nil, nil
	}
	if _, ok := raw.([]any); !ok {
		return nil, errors.New("must be an array")
	}

	data, err := json.Marshal(raw)
	if err != nil {
		return nil, err
	}
	var profiles []QualityProfile
	if err := json.Unmarshal(data, &profiles); err != nil {
		return nil, err
	}
	return profiles, nil
}

func (q *QualityConfig) Validate() error {
	if len(q.CustomFormats) > maxCustomFormats {
		return fmt.Errorf("maximum %d custom formats allowed", maxCustomFormats)
	}
	seenFormats := make(map[string]bool)
	for i := range q.CustomFormats {
		format := &q.CustomFormats[i]
		format.Name = strings.TrimSpace(format.Name)
		format.Regex = strings.TrimSpace(format.Regex)
		if format.Name == "" {
			return errors.New("custom format name cannot be empty")
		}
		if format.Regex == "" {
			return fmt.Errorf("custom format %s regex cannot be empty", format.Name)
		}
		if len(format.Name) > maxProfileLabelBytes || len(format.Regex) > maxProfileRegexBytes {
			return fmt.Errorf("custom format %s exceeds its size limit", format.Name)
		}
		key := strings.ToLower(format.Name)
		if seenFormats[key] {
			return fmt.Errorf("duplicate custom format name: %s", format.Name)
		}
		seenFormats[key] = true
		compiled, err := regexp.Compile(format.Regex)
		if err != nil {
			return fmt.Errorf("invalid regex in custom format %s: %w", format.Name, err)
		}
		format.match = compiled
	}
	if !q.EnableProfiles {
		return nil
	}
	if len(q.Profiles) == 0 {
		return errors.New("at least one quality profile is required when profiles are enabled")
	}
	if len(q.Profiles) > maxQualityProfiles {
		return fmt.Errorf("maximum %d quality profiles allowed", maxQualityProfiles)
	}
	seen := make(map[string]bool)
	for i := range q.Profiles {
		p := &q.Profiles[i]
		p.Label = strings.TrimSpace(p.Label)
		if p.Label == "" {
			return fmt.Errorf("profile label cannot be empty")
		}
		if len(p.Label) > maxProfileLabelBytes {
			return fmt.Errorf("profile label exceeds %d bytes", maxProfileLabelBytes)
		}
		for name, value := range map[string]string{
			"resolution":  p.Resolution,
			"codec_video": p.CodecVideo,
			"codec_audio": p.CodecAudio,
			"hdr":         p.HDR,
			"exclude_hdr": p.ExcludeHDR,
		} {
			if len(value) > maxProfileAttributeBytes {
				return fmt.Errorf("%s in profile %s exceeds %d bytes", name, p.Label, maxProfileAttributeBytes)
			}
		}
		if len(p.IncludeRegex) > maxProfileRegexBytes || len(p.ExcludeRegex) > maxProfileRegexBytes {
			return fmt.Errorf("profile %s regex exceeds %d bytes", p.Label, maxProfileRegexBytes)
		}
		if p.PreferredOrder < 0 || p.PreferredOrder > maxPreferredOrder {
			return fmt.Errorf("preferred_order in profile %s must be between 0 and %d", p.Label, maxPreferredOrder)
		}
		lower := strings.ToLower(p.Label)
		if seen[lower] {
			return fmt.Errorf("duplicate profile label: %s", p.Label)
		}
		seen[lower] = true

		if p.IncludeRegex != "" {
			r, err := regexp.Compile(p.IncludeRegex)
			if err != nil {
				return fmt.Errorf("invalid include_regex in profile %s: %w", p.Label, err)
			}
			p.include = r
		}
		if p.ExcludeRegex != "" {
			r, err := regexp.Compile(p.ExcludeRegex)
			if err != nil {
				return fmt.Errorf("invalid exclude_regex in profile %s: %w", p.Label, err)
			}
			p.exclude = r
		}
	}
	sort.SliceStable(q.Profiles, func(i, j int) bool {
		left, right := q.Profiles[i].PreferredOrder, q.Profiles[j].PreferredOrder
		switch {
		case left == 0 && right == 0:
			return false
		case left == 0:
			return false
		case right == 0:
			return true
		default:
			return left < right
		}
	})
	return nil
}
