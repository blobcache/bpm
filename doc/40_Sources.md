# Sources
A *Source* is something that has labeled assets, which can be downloaded.

*Sources* represent a source of truth about the labels applied to an asset.
*Sources* must only allow secure communication to trusted parties.

Sources can be referred to using URLs which look like `<type>:<path>` e.g. `github:blobcache/bpm`.

To search a source, use the `fetch` and `search` commands.
```
$ bpm search --fetch `github:protocolbuffers/protobuf`
```

## Source Types

### `github`
The `github` source type uses the GitHub REST API to list release assets and git zipfiles.

e.g. `github:blobcache/bpm`

This source assumes trust in GitHub, and whatever certificate authorities signed GitHub cert.

### `http`
The `http` source type uses https to access a page, and list all of the links on the page as assets.
This source type only communicates using https.

e.g. `http:go.dev/dl`

This source assumes trust in the remote HTTP server, and whatever certificate authorities signed the server's certificate.