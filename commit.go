package bpm

import (
	"context"
	"database/sql"
	"time"

	"github.com/blobcache/bpm/internal/dbutil"
	"github.com/blobcache/glfs"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/jmoiron/sqlx"
)

type Commit struct {
	ID uint64 `json:"id"`

	Snapshot  SnapshotID `json:"snapshot"`
	CreatedAt time.Time  `json:"created_at"`
}

func (r *Repo) GetCurrent(ctx context.Context) (*Commit, error) {
	var commitID sql.NullInt64
	err := r.db.GetContext(ctx, &commitID, `SELECT max(id) FROM commits`)
	if err != nil {
		return nil, err
	}
	if !commitID.Valid {
		return nil, nil
	}
	return r.GetCommit(ctx, uint64(commitID.Int64))
}

func (r *Repo) GetCommit(ctx context.Context, commitID uint64) (*Commit, error) {
	return dbutil.DoTx1(ctx, r.db, func(tx *sqlx.Tx) (*Commit, error) {
		return getCommit(tx, commitID)
	})
}

func (r *Repo) ListCommits(ctx context.Context) ([]Commit, error) {
	var rows []struct {
		ID         uint64    `db:"cid"`
		SnapshotID []byte    `db:"sid"`
		CreatedAt  time.Time `db:"created_at"`
	}
	if err := r.db.SelectContext(ctx, &rows, `SELECT commits.id as cid, snapshots.cid as sid, created_at FROM commits
		JOIN snapshots on commits.snapshot_id = snapshots.id
	`); err != nil {
		return nil, err
	}
	var ret []Commit
	for _, row := range rows {
		ret = append(ret, Commit{
			ID:        row.ID,
			Snapshot:  cadata.IDFromBytes(row.SnapshotID),
			CreatedAt: row.CreatedAt,
		})
	}
	return ret, nil
}

// Deploy creates a new commit, and deploys the snapshot to the filesystem
func (r *Repo) Deploy(ctx context.Context, id SnapshotID) (*Commit, error) {
	next, err := dbutil.DoTx1(ctx, r.db, func(tx *sqlx.Tx) (*Commit, error) {
		sIntID, err := lookupSnapshotIntID(tx, id)
		if err != nil {
			return nil, err
		}
		var commitID uint64
		if err := tx.Get(&commitID, `INSERT INTO commits (snapshot_id, created_at) VALUES (?, ?) RETURNING (id)`, sIntID, time.Now()); err != nil {
			return nil, err
		}
		return getCommit(tx, commitID)
	})
	if err != nil {
		return nil, err
	}
	snap, err := r.GetSnapshot(ctx, next.Snapshot)
	if err != nil {
		return nil, err
	}
	if err := r.actualize(ctx, snap.TLDs); err != nil {
		return nil, err
	}
	return next, nil
}

func (r *Repo) Modfiy(ctx context.Context, fn func(tlds map[string]glfs.Ref) error) (*Commit, error) {
	commit, err := r.GetCurrent(ctx)
	if err != nil {
		return nil, err
	}
	var next SnapshotID
	if commit != nil {
		snap, err := r.ModifySnapshot(ctx, commit.Snapshot, fn)
		if err != nil {
			return nil, err
		}
		next = *snap
	} else {
		tlds := make(map[string]glfs.Ref)
		if err := fn(tlds); err != nil {
			return nil, err
		}
		snap, err := r.PostSnapshot(ctx, tlds)
		if err != nil {
			return nil, err
		}
		next = *snap
	}
	return r.Deploy(ctx, next)
}

func getCommit(tx *sqlx.Tx, id uint64) (*Commit, error) {
	var row struct {
		SnapshotID cadata.ID `db:"snapshot_id"`
		CreatedAt  time.Time `db:"created_at"`
	}
	if err := tx.Get(&row, `SELECT snapshots.cid as snapshot_id, commits.created_at as created_at FROM commits
		JOIN snapshots ON commits.snapshot_id = snapshots.id
		WHERE commits.id = ?`, id); err != nil {
		return nil, err
	}
	return &Commit{
		ID:        id,
		Snapshot:  row.SnapshotID,
		CreatedAt: row.CreatedAt,
	}, nil
}
