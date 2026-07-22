# Silo Virtual Library

Silo Virtual Library is a zero-storage playback plugin for Silo Server. Silo stores lightweight virtual media references such as `aiostreams://movie/tt0133093`; no video files, manifests, or segments are persisted by the plugin. At playback time, the plugin asks the configured AIOStreams instance for a current upstream streaming URL.

## Install in Silo

In Silo, open **Admin → Plugins → Catalog**, add the following custom repository URL, and install **Silo Virtual Library**:

```text
https://raw.githubusercontent.com/drondeseries/silo-virtual-library/main/catalog.json
```


## How it works

1. A Silo library item registered through the authenticated plugin control plane points at an `aiostreams://` virtual path.
2. Silo delegates the path to the plugin over gRPC.
3. The playback handler recognizes the scheme and asks an `aioStreamsResolver` for a stream.
4. The resolver returns a time-limited HLS URL for immediate playback.
5. Silo or its client streams from that URL; this plugin stores no media locally.

After installation, configure the plugin's **AIOStreams Manifest URL** in Silo. The plugin derives the Stremio stream endpoint from that URL, requests streams for the IMDb identifier, and returns the first valid HTTP or HTTPS source. Manifest credentials are held in Silo-managed secret configuration and must not be committed to the repository.

When Virtual Library is selected as a request connection, compatible Silo builds automatically populate the generic Base URL and API key fields with plugin-managed values. Administrators only need to save the connection and configure the plugin settings described above.

## Requests and monitored media

The `request_router.v1` capability checks release availability before reporting a request complete. Movies use TMDB digital/physical release dates across every returned market when a TMDB token and ID are available; theatrical-only titles remain queued. Cinemeta supplies the conservative fallback release date. Once home-media availability is established, Silo registers the item immediately without waiting for AIOStreams discovery.

Items that are upcoming or theatrical-only are persisted in the configured monitored queue file. The `monitor-media` scheduled task rechecks release metadata. Silo's subsequent request-status poll observes `completed` once the title has a digital or physical release. Configure a writable absolute queue path for deployments whose plugin working directory is ephemeral.

Only an explicit user request sends asynchronous prewarm lookups to AIOStreams: one lookup for a movie and one per already-aired episode for a series. Registration does not wait for those lookups. Future episodes of an ongoing series are added on schedule without prewarming; playback always performs a fresh resolution.

Movie runtimes come from TMDB movie details when configured, with Cinemeta as fallback. Episode runtimes come from Cinemeta or TVMaze. Silo stores the canonical runtime before playback so growing HLS playlists do not make the seek bar expand second by second.

When an item becomes playable, the plugin submits a typed virtual-media registration to Silo's authenticated RuntimeHost service. Silo validates the selected library and transactionally owns all catalog, episode, virtual-file, cache-invalidation, and metadata-refresh behavior. The plugin never receives database credentials, executes SQL, or creates `.strm` files.

The server administrator configures the AIOStreams manifest URL, TMDB token, Movies library ID, and Series library ID in the plugin settings. Normal users only interact with Request and Play.

## SDK compatibility

This project targets the additive RuntimeHost extension in `github.com/drondeseries/silo-plugin-sdk` v0.10.1-virtual.2. Generated protobuf types are published at `pkg/pluginproto/silo/plugin/v1` (imported as `pb`) and request interception is provided by the `HttpRoutes` gRPC service. The extension remains wire-compatible with the v0.10 RuntimeHost contract while adding canonical virtual-media runtime metadata.

## Development

The module requires Go 1.26 or newer.

```sh
go mod download
go test ./...
go build -o bin/silo-virtual-library .
```

To inspect the manifest emitted by the compiled plugin:

```sh
./bin/silo-virtual-library manifest
```

The binary runs as a HashiCorp go-plugin subprocess and is normally started by Silo Server, not launched directly from an interactive shell.

## Production considerations

- Validate virtual identifiers and authorize playback against the requesting Silo user.
- Apply resolver timeouts, retry limits, and structured error mapping.
- Avoid logging resolver credentials, signed URLs, or access tokens.
- Return URLs with short expirations and scope them to one media item or playback session.
- Confirm AIOStreams usage and content access comply with applicable provider terms and law.

## License

No license has been selected for this starter project.
