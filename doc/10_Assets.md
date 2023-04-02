# Assets

An asset is a [Git-Like Filesystem](https://github.com/blobcache/glfs) object: either a *Blob* or a *Tree*.

Assets have an identity, which is an integer.
This identity is mostly used to refer to the asset unambiguously.
Most work with assets is done by querying for them by labels.
