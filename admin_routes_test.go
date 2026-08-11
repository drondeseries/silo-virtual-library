package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
)

func TestQueueJSONShowsOnlyMissingMoviesAndEpisodes(t *testing.T) {
	now := time.Now()
	monitor := newMediaMonitor(nil, hclog.NewNullLogger())
	monitor.items = map[string]monitoredMedia{
		"movie:missing": {Key: "movie:missing", MediaType: "movie", Title: "Missing Movie"},
		"movie:ready":   {Key: "movie:ready", MediaType: "movie", Title: "Ready Movie", Ready: true},
		"series:ongoing": {
			Key: "series:ongoing", MediaType: "series", Title: "Ongoing Show", Ready: true,
			Episodes: []virtualEpisode{
				{Season: 1, Episode: 1, Title: "Available", Released: now.Add(-2 * time.Hour), Available: true},
				{Season: 1, Episode: 2, Title: "Missing", Released: now.Add(-time.Hour)},
				{Season: 1, Episode: 3, Title: "Future", Released: now.Add(time.Hour)},
			},
		},
		"series:complete": {
			Key: "series:complete", MediaType: "series", Title: "Complete Show", Ready: true,
			Episodes: []virtualEpisode{{Season: 1, Episode: 1, Released: now.Add(-time.Hour), Available: true}},
		},
	}

	response, err := (&adminRoutes{server: &runtimeServer{monitor: monitor}}).queueJSON()
	if err != nil {
		t.Fatal(err)
	}
	var payload struct {
		Items []monitoredMedia `json:"items"`
	}
	if err := json.Unmarshal(response.Body, &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Items) != 2 {
		t.Fatalf("items = %#v, want missing movie and ongoing show", payload.Items)
	}
	if payload.Items[0].Key != "movie:missing" || payload.Items[1].Key != "series:ongoing" {
		t.Fatalf("item keys = %q, %q", payload.Items[0].Key, payload.Items[1].Key)
	}
	if len(payload.Items[1].Episodes) != 2 || payload.Items[1].Episodes[0].Episode != 2 || payload.Items[1].Episodes[1].Episode != 3 {
		t.Fatalf("missing episodes = %#v, want S01E02 and S01E03", payload.Items[1].Episodes)
	}
}
