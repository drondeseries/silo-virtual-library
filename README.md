# Silo Virtual Library

Silo Virtual Library is a zero-storage playback plugin for Silo Server. The library database stores lightweight virtual media references such as `aiostreams://movie/tt0133093`; no video files, manifests, or segments are persisted by the plugin. At playback time, the plugin asks the configured AIOStreams instance for a current upstream streaming URL.

## Install in Silo

In Silo, open **Admin → Plugins → Catalog**, add the following custom repository URL, and install **Silo Virtual Library**:

```text
https://raw.githubusercontent.com/drondeseries/silo-virtual-library/main/catalog.json
```


## How it works

1. A database-backed Silo library item points at an `aiostreams://` virtual path.
2. Silo delegates the path to the plugin over gRPC.
3. The playback handler recognizes the scheme and asks an `aioStreamsResolver` for a stream.
4. The resolver returns a time-limited HLS URL for immediate playback.
5. Silo or its client streams from that URL; this plugin stores no media locally.

After installation, configure the plugin's **AIOStreams Manifest URL** in Silo. The plugin derives the Stremio stream endpoint from that URL, requests streams for the IMDb identifier, and returns the first valid HTTP or HTTPS source. Manifest credentials are held in Silo-managed secret configuration and must not be committed to the repository.

## SDK compatibility

This project targets `github.com/Silo-Server/silo-plugin-sdk` v0.10.0. In this version, generated protobuf types are published at `pkg/pluginproto/silo/plugin/v1` (imported as `pb`) and request interception is provided by the `HttpRoutes` gRPC service. The older `github.com/Silo-Server/silo-plugin-sdk/pb` path and a dedicated `PlaybackServer` are not present in the current SDK, so `playbackServer` implements the supported `HttpRoutesServer` interface.

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
