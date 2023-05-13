# bpm is a Package Manager

- bpm manages deploying files and trees of files, which it calls *Assets* to target paths in the filesystem, which it calls *Deployments*.
- bpm stores state in a single directory, which can be configured with the environment variable `BPM_PATH`. This defaults to `$HOME/pkg`.
- *Assets* are labeled with key value pairs, called *Labels*.  *Assets* can be searched for by label.
- bpm knows about external *Sources* of assets which you can query with `bpm search`

For more information check out the [docs](./doc/00_BPM.md)

## Getting Started
### Installing BPM
Download the bpm binary and place it somewhere on your path.

It can also be built and installed with go.
The entry point is at `cmd/bpm` in this repository.
`$GOBIN` must be on your path for this to work.
```
go install github.com/blobcache/bpm/cmd/bpm
```

### Initializing a `BPM_PATH`
Then initialize bpm in the default directory.  If you choose a different directory, you will need to set an environment variable `BPM_PATH` to that directory.
```
$ bpm init $HOME/pkg
```
bpm will only operate on directories for which it has been explicitly initialized.
These initialized directories will contain a `.bpm` directory.

Now you should be ready to go.
If you ever want to uninstall bpm without leaving anything behind, you can remove the entire `BPM_PATH`

## Installing Packages

### Search
Search is used to search a source for packages

```
$ bpm search --fetch github:protocolbuffers/protobuf '.git_tag > v0.0.5'
```

The fetch flag will access the remote over the network.
It is necessary to pass `--fetch` the first time a source is accessed.
By default, `bpm search` only accesses the local filesystem, not the network.

The first argument to `bpm search` is the source URL.
The second argument is an option query, using the jq language.
The query must resolve to a boolean.
If the boolean is true, then the result is included.


### Install
```
$ bpm install github:protocolbuffers/protobuf --id=asset/asdff
```

## Creating Packages
 
```
$ bpm create [-t <tag>] <path>
<id>
```

```
$ bpm add-tag <id> <key> <value>
```

```
$ bpm rm-tag <id> <key> <value>
```

## Distributing Packages
BPM will eventually support a way of natively distributing packages.
For now developers should continue using GitHub releases, which bpm has support for.

BPM can distribute packages over both INET256 and QUIC

```
bpm serve --private-key=./path/to/key
```

```
bpm serve-quic --private-key=./path/to/key <listen-addr>
```
