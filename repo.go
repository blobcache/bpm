package bpm

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	"github.com/blobcache/bpm/internal/dbutil"
	"github.com/blobcache/bpm/internal/porting"
	"github.com/blobcache/bpm/internal/sqlstores"
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

// DeploymentDir is the directory in the filesystem used for deployments
func (r *Repo) DeploymentDir() posixfs.FS {
	return posixfs.NewFiltered(r.dir, func(x string) bool {
		isInternal := strings.HasPrefix(x, ".bpm/") || x == ".bpm"
		return !isInternal
	})
}

// actualize ensures that the filesystem matches tlds
func (r *Repo) actualize(ctx context.Context, tlds map[string]glfs.Ref) error {
	dirfs := r.DeploymentDir()
	exp := porting.NewExporter(dirfs, fsCache{r.db}, true)
	for path, ref := range tlds {
		path := path
		ref := ref
		storeID, err := dbutil.DoTx1(ctx, r.db, func(tx *sqlx.Tx) (uint64, error) {
			aid, err := lookupAssetByRoot(tx, ref)
			if err != nil {
				return 0, err
			}
			return getAssetStore(tx, aid)
		})
		if err != nil {
			return err
		}
		s := sqlstores.NewStore(r.db, Hash, MaxBlobSize, storeID)
		logctx.Infof(ctx, "exporting %v => %v", path, ref.CID)
		if err := exp.Export(ctx, s, path, ref); err != nil {
			return err
		}
	}
	return nil
}

type fsCache struct {
	db *sqlx.DB
}

func (c fsCache) Get(ctx context.Context, p string) (*porting.CacheEntry, error) {
	return nil, nil
}

func (c fsCache) Put(ctx context.Context, p string, ent porting.CacheEntry) error {
	return nil
}
