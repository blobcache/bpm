package porting

import (
	"context"
	"fmt"
	"io"
	"path"
	"time"

	"github.com/blobcache/glfs"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-state/posixfs"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

type CacheEntry struct {
	Ref        glfs.Ref
	ModifiedAt time.Time
}

type Cache interface {
	Put(ctx context.Context, p string, ent CacheEntry) error
	Get(ctx context.Context, p string) (*CacheEntry, error)
}

type NullCache struct{}

func (c NullCache) Get(ctx context.Context, p string) (*CacheEntry, error) {
	return nil, nil
}

func (c NullCache) Put(ctx context.Context, p string, ent *CacheEntry) error {
	return nil
}

type Exporter struct {
	fs        posixfs.FS
	cache     Cache
	overwrite bool
	fsop      glfs.Operator

	sem *semaphore.Weighted
}

func NewExporter(fs posixfs.FS, cache Cache, overwrite bool) *Exporter {
	return &Exporter{
		overwrite: overwrite,
		fs:        fs,
		fsop:      glfs.NewOperator(),
		cache:     cache,

		sem: semaphore.NewWeighted(50),
	}
}

func (e *Exporter) Export(ctx context.Context, s cadata.Store, p string, ref glfs.Ref) error {
	if err := e.sem.Acquire(ctx, 1); err != nil {
		return err
	}
	defer e.sem.Release(1)
	switch ref.Type {
	case glfs.TypeTree:
		return e.exportTree(ctx, s, p, ref, 0o755)
	case glfs.TypeBlob:
		return e.exportBlob(ctx, s, p, ref, 0o644)
	default:
		return fmt.Errorf("unrecognized type %q", ref.Type)
	}
}

func (e *Exporter) exportTree(ctx context.Context, s cadata.Store, p string, ref glfs.Ref, mode posixfs.FileMode) error {
	tree, err := e.fsop.GetTree(ctx, s, ref)
	if err != nil {
		return err
	}
	if err := posixfs.MkdirAll(e.fs, p, mode); err != nil {
		return err
	}
	ctx, cf := context.WithCancel(ctx)
	defer cf()
	eg, ctx := errgroup.WithContext(ctx)
	// TODO: need to delete entries which don't exist
	for _, ent := range tree.Entries {
		ent := ent
		fn := func() error {
			p2 := path.Join(p, ent.Name)
			switch ent.Ref.Type {
			case glfs.TypeTree:
				return e.exportTree(ctx, s, p2, ent.Ref, ent.FileMode)
			case glfs.TypeBlob:
				return e.exportBlob(ctx, s, p2, ent.Ref, ent.FileMode)
			default:
				return fmt.Errorf("unrecognized type %q", ent.Ref.Type)
			}
		}
		if e.sem.TryAcquire(1) {
			eg.Go(func() error {
				defer e.sem.Release(1)
				return fn()
			})
		} else {
			if err := fn(); err != nil {
				cf()
				eg.Wait()
				return err
			}
		}
	}
	return eg.Wait()
}

func (e *Exporter) exportBlob(ctx context.Context, s cadata.Store, p string, ref glfs.Ref, mode posixfs.FileMode) error {
	// check cache
	finfo, err := e.fs.Stat(p)
	if err != nil && !posixfs.IsErrNotExist(err) {
		return err
	}
	ent, err := e.cache.Get(ctx, p)
	if err != nil {
		return err
	}
	if ent != nil && finfo != nil && !finfo.ModTime().After(ent.ModifiedAt) {
		return nil // skip
	}

	r, err := e.fsop.GetBlob(ctx, s, ref)
	if err != nil {
		return err
	}
	flags := posixfs.O_CREATE | posixfs.O_WRONLY
	if e.overwrite {
		flags |= posixfs.O_TRUNC
	} else {
		flags |= posixfs.O_EXCL
	}
	f, err := e.fs.OpenFile(p, flags, mode)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := io.Copy(f, r); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	// put in cache
	finfo, err = e.fs.Stat(p)
	if err != nil {
		return err
	}
	return e.cache.Put(ctx, p, CacheEntry{Ref: ref, ModifiedAt: finfo.ModTime()})
}

func deleteAll(ctx context.Context, fs posixfs.FS, p string) error {
	finfo, err := fs.Stat(p)
	if err != nil {
		if posixfs.IsErrNotExist(err) {
			err = nil
		}
		return err
	}
	if finfo.IsDir() {
		ents, err := posixfs.ReadDir(fs, p)
		if err != nil {
			return err
		}
		for _, ent := range ents {
			p2 := path.Join(p, ent.Name)
			if err := deleteAll(ctx, fs, p2); err != nil {
				return err
			}
		}
	}
	return posixfs.DeleteFile(ctx, fs, p)
}
