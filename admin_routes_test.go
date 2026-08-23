package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	pb "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"github.com/drondeseries/silo-virtual-library/pkg/release"
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

func TestAdminScheduleJSONAndRefresh(t *testing.T) {
	store := release.NewReleaseStore()
	store.SetShow("tt1234567", &release.ShowSchedule{
		IMDBID: "tt1234567",
		Status: "Running",
		Episodes: map[string]release.EpisodeInfo{
			"1:1": {Season: 1, Episode: 1, Title: "Pilot", AirDate: time.Now().Add(-24 * time.Hour)},
		},
	})

	server := &runtimeServer{
		releaseStore: store,
		scheduler:    release.NewScheduler(store, release.NewMetadataClient(nil), time.Hour),
	}
	admin := newAdminRoutes(server)

	// Test schedule GET
	getReq := &pb.HandleHTTPRequest{
		Path:    "/admin/virtual-library/schedule",
		Method:  "GET",
		Headers: map[string]string{"X-Silo-User-Role": "admin"},
	}
	resp, err := admin.Handle(context.Background(), getReq)
	if err != nil {
		t.Fatalf("Handle schedule GET error: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("schedule GET statusCode = %d, want 200", resp.StatusCode)
	}
	var schedulePayload struct {
		Count int                     `json:"count"`
		Shows []*release.ShowSchedule `json:"shows"`
	}
	if err := json.Unmarshal(resp.Body, &schedulePayload); err != nil {
		t.Fatalf("unmarshal schedule response: %v", err)
	}
	if schedulePayload.Count != 1 || len(schedulePayload.Shows) != 1 || schedulePayload.Shows[0].IMDBID != "tt1234567" {
		t.Fatalf("unexpected schedule payload: %#v", schedulePayload)
	}

	// Test forbidden for non-admin
	nonAdminReq := &pb.HandleHTTPRequest{
		Path:   "/admin/virtual-library/schedule",
		Method: "GET",
	}
	forbiddenResp, err := admin.Handle(context.Background(), nonAdminReq)
	if err != nil || forbiddenResp.StatusCode != 403 {
		t.Fatalf("expected 403 for non-admin, got resp=%v, err=%v", forbiddenResp, err)
	}
}
