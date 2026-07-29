package main

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

type QualityProfile struct {
	Label          string `json:"label"`
	Resolution     string `json:"resolution"`
	IncludeRegex   string `json:"include_regex"`
	ExcludeRegex   string `json:"exclude_regex"`
	PreferredOrder int    `json:"preferred_order"`
	CodecVideo     string `json:"codec_video"`
	CodecAudio     string `json:"codec_audio"`
	HDR            string `json:"hdr"`

	include *regexp.Regexp
	exclude *regexp.Regexp
}

type QualityConfig struct {
	EnableProfiles      bool             `json:"enable_quality_profiles"`
	Profiles            []QualityProfile `json:"quality_profiles"`
	FallbackToAnyStream bool             `json:"fallback_to_any_stream"`
}

// decodeQualityProfiles accepts both the typed array emitted by newer Silo
// admin forms and the JSON string emitted by older forms.
func decodeQualityProfiles(raw any) ([]QualityProfile, error) {
	if raw == nil {
		return nil, nil
	}
	if encoded, ok := raw.(string); ok {
		if strings.TrimSpace(encoded) == "" {
			return nil, nil
		}
		raw = json.RawMessage(encoded)
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
	if !q.EnableProfiles {
		return nil
	}
	if len(q.Profiles) > 10 {
		return fmt.Errorf("maximum 10 quality profiles allowed")
	}
	seen := make(map[string]bool)
	for i := range q.Profiles {
		p := &q.Profiles[i]
		p.Label = strings.TrimSpace(p.Label)
		if p.Label == "" {
			return fmt.Errorf("profile label cannot be empty")
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
	return nil
}
