# Deployments
Deployments are how BPM actually presents assets to the user through the filesystem.

A deployment represents exporting an asset to a directory under `BPM_PATH`.
Deployments have name, which controls where the asset is deployed.
The path to the root of the deployment is `$BPM_PATH}/$DEPLOY_NAME`.

> NOTE: 
>
> bpm calls *Deployments* "*Deploys*" for short, that is reflected in both the command line interface and source code.
> It's just because "deployment" is a long word, there is no conceptual distinction.

bpm will never deploy an asset where the user doesn't want it.
The user has to ask for a specific asset to be deployed at a specific path.

```
bpm deploy create <name> <asset-id>
```

The `entry` flag additionally creates a symlink to a file within the package
```
bpm deploy --entry=$HOME/bin:entry.sh
```
