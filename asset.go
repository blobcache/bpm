package bpm

import (
	"context"
	"encoding/json"
	"runtime"

	"github.com/blobcache/bpm/internal/dbutil"
	"github.com/blobcache/bpm/internal/slices2"
	"github.com/blobcache/bpm/internal/sqlstores"
	"github.com/blobcache/glfs"
	"github.com/brendoncarroll/go-state"
	"github.com/brendoncarroll/go-state/posixfs"
	"github.com/jmoiron/sqlx"
	"golang.org/x/sync/semaphore"
)

// CreateAsset creates an empty Asset.
func (r *Repo) CreateAsset(ctx context.Context) (uint64, error) {
	return dbutil.DoTx1(ctx, r.db, func(tx *sqlx.Tx) (uint64, error) {
		sid, err := sqlstores.CreateStore(tx)
		if err != nil {
			return 0, err
		}
		return createAsset(tx, sid)
	})
}

// CreateAssetFS creates an Asset with data from the filesystem.
func (r *Repo) CreateAssetFS(ctx context.Context, fsx posixfs.FS, p string) (uint64, error) {
	aid, err := r.CreateAsset(ctx)
	if err != nil {
		return 0, err
	}
	return aid, r.Import(ctx, aid, fsx, p)
}

// Import adds an object outside the repository into an asset.
func (r *Repo) Import(ctx context.Context, aid uint64, fsx posixfs.FS, p string) error {
	sid, err := dbutil.DoTx1(ctx, r.db, func(tx *sqlx.Tx) (uint64, error) { return getAssetStore(tx, aid) })
	if err != nil {
		return err
	}
	s := sqlstores.NewStore(r.db, Hash, MaxBlobSize, sid)
	sem := semaphore.NewWeighted(int64(runtime.GOMAXPROCS(0)))
	ref, err := GLFSImport(ctx, &r.glfsOp, sem, s, fsx, p)
	if err != nil {
		return err
	}
	if err := dbutil.DoTx(ctx, r.db, func(tx *sqlx.Tx) error {
		return putAssetRef(tx, aid, *ref)
	}); err != nil {
		return err
	}
	return nil
}

// PutTags inserts or replaces each label.
func (r *Repo) PutLabels(ctx context.Context, aid uint64, labels []Label) error {
	return dbutil.DoTx(ctx, r.db, func(tx *sqlx.Tx) error {
		for _, l := range labels {
			if _, err := tx.Exec(`INSERT INTO asset_labels (asset_id, k, v)`, aid, l.Key, l.Value); err != nil {
				return err
			}
		}
		return nil
	})
}

// GetTags returns the labels for an asset
func (r *Repo) GetLabels(ctx context.Context, aid uint64) ([]Label, error) {
	return dbutil.DoTx1(ctx, r.db, func(tx *sqlx.Tx) (ret []Label, _ error) {
		err := tx.Select(&ret, `SELECT k, v FROM asset_labels WHERE asset_id = ?`, aid)
		return ret, err
	})
}

func (r *Repo) GetAsset(ctx context.Context, aid uint64) (Asset, error) {
	return dbutil.DoTx1(ctx, r.db, func(tx *sqlx.Tx) (Asset, error) {
		return getAsset(tx, aid)
	})
}

func (r *Repo) ListAssets(ctx context.Context, span state.Span[uint64], limit int) ([]uint64, error) {
	return dbutil.List(ctx, r.db, `SELECT id FROM assets`, "id", dbutil.ListParams[uint64]{
		Span: span,

		Limit:        limit,
		DefaultLimit: 1000,
		MaxLimit:     10_000,
	})
}

func (r *Repo) ListAssetsFull(ctx context.Context, span state.Span[uint64], limit int) ([]Asset, error) {
	ids, err := r.ListAssets(ctx, span, limit)
	if err != nil {
		return nil, err
	}
	sem := semaphore.NewWeighted(10)
	return slices2.ParMapErr(ctx, sem, ids, func(ctx context.Context, id uint64) (Asset, error) {
		return dbutil.DoTx1(ctx, r.db, func(tx *sqlx.Tx) (Asset, error) {
			return getAsset(tx, id)
		})
	})
}

func createAsset(tx *sqlx.Tx, storeID uint64) (uint64, error) {
	var aid uint64
	err := tx.Get(&aid, `INSERT INTO assets (store_id) VALUES (?) RETURNING id`, storeID)
	return aid, err
}

func getAsset(tx *sqlx.Tx, id uint64) (Asset, error) {
	ref, err := getAssetRef(tx, id)
	if err != nil {
		return Asset{}, err
	}
	ls, err := getLabelSet(tx, id)
	if err != nil {
		return Asset{}, err
	}
	us, err := lookupUpstream(tx, id)
	if err != nil {
		return Asset{}, err
	}
	return Asset{
		ID:       id,
		Root:     *ref,
		Labels:   ls,
		Upstream: us,
	}, nil
}

func getAssetStore(tx *sqlx.Tx, aid uint64) (ret uint64, _ error) {
	return ret, tx.Get(&ret, `SELECT store_id FROM assets WHERE id = ?`, aid)
}

func putAssetRef(tx *sqlx.Tx, aid uint64, ref glfs.Ref) error {
	data, err := json.Marshal(ref)
	if err != nil {
		return err
	}
	_, err = tx.Exec(`UPDATE assets SET root = ? WHERE id = ?`, data, aid)
	return err
}

func getAssetRef(tx *sqlx.Tx, aid uint64) (*glfs.Ref, error) {
	var data []byte
	if err := tx.Get(&data, `SELECT root FROM assets WHERE id = ?`, aid); err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return &glfs.Ref{}, nil
	}
	var ref glfs.Ref
	if err := json.Unmarshal(data, &ref); err != nil {
		return nil, err
	}
	return &ref, nil
}

func getLabelSet(tx *sqlx.Tx, aid uint64) (LabelSet, error) {
	var rows []struct {
		Key   string `db:"k"`
		Value string `db:"v"`
	}
	if err := tx.Select(&rows, `SELECT asset_labels.k, asset_labels.v FROM asset_labels WHERE asset_id = ?`, aid); err != nil {
		return nil, err
	}
	ls := LabelSet{}
	for _, row := range rows {
		ls[row.Key] = row.Value
	}
	return ls, nil
}

// putLabelSet
func putLabelSet(tx *sqlx.Tx, aid uint64, labels LabelSet) error {
	for k, v := range labels {
		if _, err := tx.Exec(`INSERT OR REPLACE INTO asset_labels (asset_id, k, v) VALUES (?, ?, ?)`, aid, k, v); err != nil {
			return err
		}
	}
	return nil
}

func lookupAssetByRoot(tx *sqlx.Tx, root glfs.Ref) (uint64, error) {
	data, err := json.Marshal(root)
	if err != nil {
		return 0, err
	}
	var aid uint64
	err = tx.Get(&aid, `SELECT id FROM assets WHERE root = ?`, data)
	return aid, err
}
