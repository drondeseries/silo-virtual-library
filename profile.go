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
	maxProfileLabelBytes     = 128
	maxProfileRegexBytes     = 1024
	maxProfileAttributeBytes = 64
	maxPreferredOrder        = 10000
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
