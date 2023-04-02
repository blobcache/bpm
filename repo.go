package bpm

import (
	"context"
	"os"
	"path/filepath"

	"github.com/blobcache/bpm/internal/dbutil"
	"github.com/blobcache/bpm/sources"
	"github.com/blobcache/glfs"
	"github.com/brendoncarroll/go-state/posixfs"
	"github.com/brendoncarroll/stdctx/logctx"
	"github.com/jmoiron/sqlx"
)

const bpmPath = ".bpm"

type Repo struct {
	db     *sqlx.DB
	dir    posixfs.FS
	glfsOp glfs.Operator
}

func New(db *sqlx.DB, dir posixfs.FS) *Repo {
	return &Repo{
		db:  db,
		dir: dir,

		glfsOp: glfs.NewOperator(),
	}
}

// Init creates a new repo under the path p, which must be a directory
func Init(ctx context.Context, p string) error {
	logctx.Infof(ctx, "initializing repo at %q", p)
	if err := os.Mkdir(filepath.Join(p, bpmPath), 0o755); err != nil {
		return err
	}
	return nil
}

// Open opens the repo in the directory at p
func Open(p string) (*Repo, error) {
	_, err := os.Stat(filepath.Join(p, bpmPath))
	if err != nil {
		return nil, err
	}
	dbPath := filepath.Join(p, bpmPath, "bpm.db")
	db, err := dbutil.OpenDB(dbPath)
	if err != nil {
		return nil, err
	}
	if err := setupDB(context.Background(), db); err != nil {
		return nil, err
	}
	return New(db, posixfs.NewDirFS(p)), nil
}

func (r *Repo) LocalSource() sources.Source {
	panic("searching the local assets not yet implemented")
}
