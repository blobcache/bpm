package bpm

import (
	"context"

	"github.com/blobcache/glfs"
	"github.com/blobcache/glfs/glfsposix"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-state/posixfs"
	"golang.org/x/sync/semaphore"
)

// GLFSImport goes from a POSIX filesystem to GLFS
func GLFSImport(ctx context.Context, op *glfs.Operator, sem *semaphore.Weighted, s cadata.Poster, fsx posixfs.FS, p string) (*glfs.Ref, error) {
	return glfsposix.Import(ctx, op, sem, s, fsx, p)
}

// GLFSExport exports a glfs object beneath p in the filesystem fsx.
func GLFSExport(ctx context.Context, op *glfs.Operator, sem *semaphore.Weighted, s cadata.Getter, root glfs.Ref, fsx posixfs.FS, p string) error {
	return glfsposix.Export(ctx, op, sem, s, root, fsx, p)
}
