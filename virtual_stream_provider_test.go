package main

import (
	"testing"

	pb "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
)

func TestVirtualPathForRequestUsesProviderIDs(t *testing.T) {
	cases := []struct {
		name string
		req  *pb.ResolveVirtualStreamRequest
		want string
	}{
		{"movie imdb", &pb.ResolveVirtualStreamRequest{MediaType: "movie", ExternalIds: map[string]string{"imdb": "tt123"}}, "virtual://movie/tt123"},
		{"episode tvdb", &pb.ResolveVirtualStreamRequest{MediaType: "episode", ExternalIds: map[string]string{"tvdb": "456"}, SeasonNumber: 2, EpisodeNumber: 3}, "virtual://series/tvdb:456/2/3"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := virtualPathForRequest(tc.req)
			if err != nil || got != tc.want {
				t.Fatalf("virtualPathForRequest() = %q, %v; want %q", got, err, tc.want)
			}
		})
	}
}

func TestVirtualPathForRequestRequiresEpisodeCoordinates(t *testing.T) {
	_, err := virtualPathForRequest(&pb.ResolveVirtualStreamRequest{MediaType: "episode", ExternalIds: map[string]string{"imdb": "tt123"}})
	if err == nil {
		t.Fatal("expected missing season/episode error")
	}
}
