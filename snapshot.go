package bpm

import (
	"context"
	"database/sql"
	"encoding/json"
	"regexp"

	"github.com/blobcache/glfs"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"

	"github.com/blobcache/bpm/internal/dbutil"
)

type SnapshotID = cadata.ID

type Snapshot struct {
	ID   SnapshotID          `json:"id"`
	TLDs map[string]glfs.Ref `json:"tlds"`
}

func (r *Repo) PostSnapshot(ctx context.Context, tlds map[string]glfs.Ref) (*SnapshotID, error) {
	return dbutil.DoTx1(ctx, r.db, func(tx *sqlx.Tx) (*SnapshotID, error) { return postSnapshot(tx, tlds) })
}

func (r *Repo) GetSnapshot(ctx context.Context, id SnapshotID) (*Snapshot, error) {
	return dbutil.DoTx1(ctx, r.db, func(tx *sqlx.Tx) (*Snapshot, error) {
		return getSnapshot(tx, id)
	})
}

func (r *Repo) ListSnapshots(ctx context.Context) ([]SnapshotID, error) {
	return dbutil.DoTx1(ctx, r.db, func(tx *sqlx.Tx) ([]SnapshotID, error) {
		var ret []SnapshotID
		return ret, tx.Select(&ret, `SELECT cid FROM snapshots`)
	})
}

func (r *Repo) ListSnapshotsFull(ctx context.Context) ([]Snapshot, error) {
	ids, err := r.ListSnapshots(ctx)
	if err != nil {
		return nil, err
	}
	return dbutil.DoTx1(ctx, r.db, func(tx *sqlx.Tx) (ret []Snapshot, _ error) {
		for _, id := range ids {
			d, err := getSnapshot(tx, id)
			if err != nil {
				return nil, err
			}
			ret = append(ret, *d)
		}
		return ret, nil
	})
}

func (r *Repo) ModifySnapshot(ctx context.Context, id SnapshotID, fn func(tlds map[string]glfs.Ref) error) (*SnapshotID, error) {
	snap, err := r.GetSnapshot(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := fn(snap.TLDs); err != nil {
		return nil, err
	}
	return r.PostSnapshot(ctx, snap.TLDs)
}

var tldNameRe = regexp.MustCompile(`^[a-z|A-Z|0-9_\-]+$`)

func checkTLDPath(x string) error {
	if !tldNameRe.MatchString(x) {
		return errors.Errorf("invalid top level directory name %q", x)
	}
	return nil
}

func lookupSnapshotIntID(tx *sqlx.Tx, id SnapshotID) (ret uint64, err error) {
	err = tx.Get(&ret, `SELECT id FROM snapshots WHERE cid = ?`, id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return ret, err
}

func postSnapshot(tx *sqlx.Tx, tlds map[string]glfs.Ref) (*SnapshotID, error) {
	id, err := computeSnapshotID(tlds)
	if err != nil {
		return nil, err
	}
	var dbid uint64
	if err := tx.Get(&dbid, `SELECT id FROM snapshots WHERE cid = ?`, id); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
		if err := tx.Get(&dbid, `INSERT INTO snapshots (cid) VALUES (?) RETURNING id`, id); err != nil {
			return nil, err
		}
		if err := putSnapshotTLDs(tx, dbid, tlds); err != nil {
			return nil, err
		}
	}
	return id, nil
}

func getSnapshot(tx *sqlx.Tx, id SnapshotID) (*Snapshot, error) {
	var dbid uint64
	if err := tx.Get(&dbid, `SELECT id FROM snapshots WHERE cid = ?`, id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	tlds, err := getSnapshotTLDs(tx, dbid)
	if err != nil {
		return nil, err
	}
	return &Snapshot{
		ID:   id,
		TLDs: tlds,
	}, nil
}

func putSnapshotTLDs(tx *sqlx.Tx, dbid uint64, tlds map[string]glfs.Ref) error {
	for path, root := range tlds {
		if err := checkTLDPath(path); err != nil {
			return err
		}
		data, err := json.Marshal(root)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO snapshot_tlds (snapshot_id, path, root) VALUES (?, ?, ?)`, dbid, path, data); err != nil {
			return err
		}
	}
	return nil
}

func getSnapshotTLDs(tx *sqlx.Tx, dbid uint64) (map[string]glfs.Ref, error) {
	var tldRows []struct {
		Path string `db:"path"`
		Root []byte `db:"root"`
	}
	if err := tx.Select(&tldRows, `SELECT path, root FROM snapshot_tlds WHERE snapshot_id = ?`, dbid); err != nil {
		return nil, err
	}

	ret := make(map[string]glfs.Ref)
	for _, ar := range tldRows {
		var root glfs.Ref
		if err := json.Unmarshal(ar.Root, &root); err != nil {
			return nil, err
		}
		ret[ar.Path] = root
	}
	return ret, nil
}

func computeSnapshotID(tlds map[string]glfs.Ref) (*SnapshotID, error) {
	s := cadata.NewVoid(Hash, MaxBlobSize)
	op := glfs.NewOperator()
	ref, err := op.PostTreeFromMap(context.Background(), s, tlds)
	if err != nil {
		return nil, err
	}
	cid := ref.CID
	return &cid, nil
}
