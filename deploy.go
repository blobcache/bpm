package bpm

import (
	"context"
	"database/sql"
	"encoding/json"
	"path"
	"regexp"
	"runtime"
	"strings"
	"time"

	"github.com/blobcache/glfs"
	"github.com/blobcache/glfs/glfsposix"
	"github.com/brendoncarroll/go-state/posixfs"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/blobcache/bpm/internal/dbutil"
	"github.com/blobcache/bpm/internal/sqlstores"
)

func (r *Repo) GetDeploy(ctx context.Context, id uint64) (*Deploy, error) {
	return dbutil.DoTx1(ctx, r.db, func(tx *sqlx.Tx) (*Deploy, error) {
		if id == 0 {
			var err error
			if id, err = lookupCurrentDeploy(tx); err != nil {
				return nil, err
			}
			if id == 0 {
				return nil, nil
			}
		}
		return getDeploy(tx, id)
	})
}

func (r *Repo) ListDeploys(ctx context.Context) ([]uint64, error) {
	return dbutil.DoTx1(ctx, r.db, func(tx *sqlx.Tx) ([]uint64, error) {
		var ret []uint64
		return ret, tx.Select(&ret, `SELECT id FROM deploys`)
	})
}

func (r *Repo) ListDeploysFull(ctx context.Context) ([]Deploy, error) {
	ids, err := r.ListDeploys(ctx)
	if err != nil {
		return nil, err
	}
	return dbutil.DoTx1(ctx, r.db, func(tx *sqlx.Tx) (ret []Deploy, _ error) {
		for _, id := range ids {
			d, err := getDeploy(tx, id)
			if err != nil {
				return nil, err
			}
			ret = append(ret, *d)
		}
		return ret, nil
	})
}

// Deploy creates a new deployment
func (r *Repo) Deploy(ctx context.Context, assets map[string]uint64) (uint64, error) {
	pathRefs := map[string]glfs.Ref{}
	for path, aid := range assets {
		if err := checkDeployPath(path); err != nil {
			return 0, err
		}
		ref, err := dbutil.DoTx1(ctx, r.db, func(tx *sqlx.Tx) (*glfs.Ref, error) {
			return getAssetRef(tx, aid)
		})
		if err != nil {
			return 0, err
		}
		pathRefs[path] = *ref
	}
	prev, err := r.GetDeploy(ctx, 0)
	if err != nil {
		return 0, err
	}
	did, err := dbutil.DoTx1(ctx, r.db, func(tx *sqlx.Tx) (uint64, error) {
		currentID, err := lookupCurrentDeploy(tx)
		if err != nil {
			return 0, err
		}
		if _, err := tx.Exec(`UPDATE deploys SET teardown_at = ? WHERE id = ?`, time.Now(), currentID); err != nil {
			return 0, err
		}
		var did uint64
		if err := tx.Get(&did, `INSERT INTO deploys (created_at) VALUES (?) RETURNING (id)`, time.Now()); err != nil {
			return 0, err
		}
		if err := putDeployAssets(tx, did, assets); err != nil {
			return 0, err
		}
		return did, nil
	})
	if err != nil {
		return 0, err
	}

	// export to filesystem
	dirfs := r.DeploymentDir()
	sem := semaphore.NewWeighted(int64(runtime.GOMAXPROCS(0)))
	var eg errgroup.Group
	for path, ref := range pathRefs {
		path := path
		ref := ref
		aid := assets[path]
		// skip assets which already exist
		if prev != nil && prev.Assets[path].Root.Equals(ref) {
			continue
		}
		eg.Go(func() error {
			storeID, err := dbutil.DoTx1(ctx, r.db, func(tx *sqlx.Tx) (uint64, error) { return getAssetStore(tx, aid) })
			if err != nil {
				return err
			}
			s := sqlstores.NewStore(r.db, Hash, MaxBlobSize, storeID)
			return glfsposix.Export(ctx, &r.glfsOp, sem, s, ref, dirfs, path)
		})
	}
	return did, eg.Wait()
}

// DeploymentDir is the directory in the filesystem used for deployments
func (r *Repo) DeploymentDir() posixfs.FS {
	return posixfs.NewFiltered(r.dir, func(x string) bool {
		isInternal := strings.HasPrefix(x, ".bpm/") || x == ".bpm"
		return !isInternal
	})
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

var deployNameRe = regexp.MustCompile(`^[a-z|A-Z|0-9_\-]+$`)

func checkDeployPath(x string) error {
	if !deployNameRe.MatchString(x) {
		return errors.Errorf("invalid deploy name %q", x)
	}
	return nil
}

func lookupCurrentDeploy(tx *sqlx.Tx) (ret uint64, err error) {
	err = tx.Get(&ret, `SELECT id FROM deploys WHERE teardown_at IS NULL LIMIT 1`)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return ret, err
}

func putDeployAssets(tx *sqlx.Tx, id uint64, assets map[string]uint64) error {
	for path, aid := range assets {
		if _, err := tx.Exec(`INSERT INTO deploy_items (deploy_id, path, asset_id) VALUES (?, ?, ?)`, id, path, aid); err != nil {
			return err
		}
	}
	return nil
}

func getDeploy(tx *sqlx.Tx, id uint64) (*Deploy, error) {
	var row struct {
		ID         uint64       `db:"id"`
		CreatedAt  time.Time    `db:"created_at"`
		TeardownAt sql.NullTime `db:"teardown_at"`
	}
	if err := tx.Get(&row, `SELECT id, created_at, teardown_at FROM deploys WHERE id = ?`, id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	var assetRows []struct {
		ID   uint64 `db:"id"`
		Path string `db:"path"`
		Root []byte `db:"root"`
	}
	if err := tx.Select(&assetRows, `SELECT assets.id as id, path, assets.root as root
		FROM deploy_items JOIN assets ON deploy_items.asset_id = assets.id
		WHERE deploy_items.deploy_id = ? `, id); err != nil {
		return nil, err
	}
	d := Deploy{
		ID:        row.ID,
		CreatedAt: row.CreatedAt,
		Assets:    make(map[string]Asset),
	}
	if row.TeardownAt.Valid {
		d.TeardownAt = &row.TeardownAt.Time
	}
	for _, ar := range assetRows {
		var root glfs.Ref
		json.Unmarshal(ar.Root, &root)
		d.Assets[ar.Path] = Asset{
			ID:   ar.ID,
			Root: root,
		}
	}
	return &d, nil
}
