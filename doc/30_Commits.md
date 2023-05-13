# Commits
Commits are how BPM actually presents assets to the user through the filesystem.

A commit represents exporting a snapshot (the desired state) of the bpm directory to the filesystem (the actual state).

bpm will never deploy an asset where the user doesn't want it.
The user has to ask for a specific asset to be deployed at a specific path.

Commits can be created using the `install` `deploy`, and `remove` commands.

```
bpm install <name> <source> <remote-id>
```

e.g.
```
$ bpm install go-src-v1.20 github:golang/go git-1.20
```
