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
	Preset                   string           `json:"quality_preset"`
	CustomFormatPreset       string           `json:"custom_format_preset"`
	EnableProfiles           bool             `json:"enable_quality_profiles"`
	Profiles                 []QualityProfile `json:"quality_profiles"`
	CustomFormats            []CustomFormat   `json:"custom_formats"`
	FallbackToAnyStream      bool             `json:"fallback_to_any_stream"`
	SingleStreamWithFailover bool             `json:"single_stream_with_failover"`
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

func customFormatPresets(preset string) []CustomFormat {
	switch strings.ToLower(strings.TrimSpace(preset)) {
	case "english-original":
		return []CustomFormat{
			{Name: "English", Regex: `(?i)\b(?:eng|en|english)\b`, Score: 100},
			{Name: "German", Regex: `(?i)\b(?:deu|ger|de|german|deutsch)\b`, Reject: true},
			{Name: "Dubbed", Regex: `(?i)\b(?:dub|dubbed|dublado|synchroni[sz]ed)\b`, Reject: true},
			{Name: "Multi Audio", Regex: `(?i)\b(?:multi|dual|multi[ ._-]*audio)\b`, Score: -25},
		}
	case "english-strict":
		return []CustomFormat{
			{Name: "English", Regex: `(?i)\b(?:eng|en|english)\b`, Score: 100},
			{Name: "German", Regex: `(?i)\b(?:deu|ger|de|german|deutsch)\b`, Reject: true},
			{Name: "Non-English Language", Regex: `(?i)\b(?:fra|fre|fr|ita|spa|jpn|kor|zho|chi|por|rus|ara|nld|dut|pol|swe|nor|dan|fin)\b`, Reject: true},
			{Name: "Dubbed", Regex: `(?i)\b(?:dub|dubbed|dublado|synchroni[sz]ed)\b`, Reject: true},
			{Name: "Multi Audio", Regex: `(?i)\b(?:multi|dual|multi[ ._-]*audio)\b`, Reject: true},
		}
	case "original-or-english":
		return []CustomFormat{
			{Name: "English", Regex: `(?i)\b(?:eng|en|english)\b`, Score: 100},
			{Name: "German Dub", Regex: `(?i)\b(?:german|deutsch|deu|ger)\b`, Reject: true},
			{Name: "Dubbed", Regex: `(?i)\b(?:dub|dubbed|dublado|synchroni[sz]ed)\b`, Reject: true},
			{Name: "Multi Audio", Regex: `(?i)\b(?:multi|dual|multi[ ._-]*audio)\b`, Score: -10},
		}
	case "clean-quality":
		return []CustomFormat{
			{Name: "Cam/TS/HDCam", Regex: `(?i)\b(?:cam|hdcam|telecine|tc|tele-sync|ts|hd-ts|pdvd)\b`, Reject: true},
			{Name: "3D", Regex: `(?i)\b(?:3d|sbs|tab|hsbs|htab)\b`, Reject: true},
			{Name: "BR-DISK / Iso", Regex: `(?i)\b(?:br[ ._-]*disk|iso|bdmv)\b`, Reject: true},
			{Name: "Extras / Sample", Regex: `(?i)\b(?:sample|trailer|extras)\b`, Reject: true},
		}
	case "audio-hd":
		return []CustomFormat{
			{Name: "TrueHD Atmos / DTS:X", Regex: `(?i)(?:truehd[ ._-]*atmos|dts[ ._-]*x)`, Score: 500},
			{Name: "ATMOS", Regex: `(?i)\batmos\b`, Score: 300},
			{Name: "TrueHD / DTS-HD MA", Regex: `(?i)(?:truehd|dts[ ._-]*hd[ ._-]*ma)`, Score: 250},
		}
	case "repack-proper":
		return []CustomFormat{
			{Name: "Repack / Proper", Regex: `(?i)\b(?:repack[0-9]?|proper[0-9]?|rerip)\b`, Score: 200},
		}
	case "top-web-sources":
		return []CustomFormat{
			{Name: "Top Tier WEB (NF/ATVP/AMZN/DSNP/MAX)", Regex: `(?i)\b(?:nf|atvp|atv|amzn|dsnp|hmax|max|bcore)\b`, Score: 150},
		}
	case "anime-enhanced":
		return []CustomFormat{
			{Name: "Dual Audio", Regex: `(?i)\b(?:dual[ ._-]*audio|multi[ ._-]*audio)\b`, Score: 200},
			{Name: "Uncensored", Regex: `(?i)\buncensored\b`, Score: 150},
			{Name: "10-bit Color", Regex: `(?i)\b10bit\b`, Score: 100},
		}
	case "web-tier-01":
		return []CustomFormat{
			{Name: "WEB Tier 1 Groups", Regex: `(?i)-(?:NTb|FLUX|Kitsune|ETHEL|playWEB|NOGRP|DECiBEL|DON|CtrlHD|TrollHD|KiNGS|hallowed|CRiMSON|BLUTONiUM|NIMA)\b`, Score: 300},
		}
	case "web-tier-02":
		return []CustomFormat{
			{Name: "WEB Tier 2 Groups", Regex: `(?i)-(?:QxR|SiNNERS|CasStudio|CMRG|LAZY|W4NK3R|HQMUX|GHOSTS)\b`, Score: 150},
		}
	case "remux-tier-01":
		return []CustomFormat{
			{Name: "Remux Tier 1 Groups", Regex: `(?i)-(?:FraMeSToR|EPSiLON|KRaLiMARK|HDC|DON|CtrlHD|TrollHD|BHDStudio)\b`, Score: 500},
		}
	case "remux-tier-02":
		return []CustomFormat{
			{Name: "Remux Tier 2 Groups", Regex: `(?i)-(?:BLUTONiUM|PmP|TRiToN|SNAKE|W4NK3R)\b`, Score: 250},
		}
	case "trash-recommended":
		return []CustomFormat{
			{Name: "English", Regex: `(?i)\b(?:eng|en|english)\b`, Score: 100},
			{Name: "German", Regex: `(?i)\b(?:deu|ger|de|german|deutsch)\b`, Reject: true},
			{Name: "Dubbed", Regex: `(?i)\b(?:dub|dubbed|dublado|synchroni[sz]ed)\b`, Reject: true},
			{Name: "Cam/TS/HDCam", Regex: `(?i)\b(?:cam|hdcam|telecine|tc|tele-sync|ts|hd-ts|pdvd)\b`, Reject: true},
			{Name: "3D", Regex: `(?i)\b(?:3d|sbs|tab|hsbs|htab)\b`, Reject: true},
			{Name: "BR-DISK / Iso", Regex: `(?i)\b(?:br[ ._-]*disk|iso|bdmv)\b`, Reject: true},
			{Name: "Extras / Sample", Regex: `(?i)\b(?:sample|trailer|extras)\b`, Reject: true},
			{Name: "Repack / Proper", Regex: `(?i)\b(?:repack[0-9]?|proper[0-9]?|rerip)\b`, Score: 200},
			{Name: "Remux Tier 1", Regex: `(?i)-(?:FraMeSToR|EPSiLON|KRaLiMARK|HDC|DON|CtrlHD|TrollHD|BHDStudio)\b`, Score: 500},
			{Name: "WEB Tier 1", Regex: `(?i)-(?:NTb|FLUX|Kitsune|ETHEL|playWEB|NOGRP|DECiBEL|DON|CtrlHD|TrollHD|KiNGS|hallowed|CRiMSON|BLUTONiUM|NIMA)\b`, Score: 300},
			{Name: "TrueHD Atmos / DTS:X", Regex: `(?i)(?:truehd[ ._-]*atmos|dts[ ._-]*x)`, Score: 250},
		}
	default:
		return nil
	}
}

func (q *QualityConfig) ApplyPreset() {
	if profiles := qualityPresetProfiles(q.Preset); len(profiles) > 0 {
		q.Profiles = profiles
	}
	if formats := customFormatPresets(q.CustomFormatPreset); len(formats) > 0 {
		q.CustomFormats = formats
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
