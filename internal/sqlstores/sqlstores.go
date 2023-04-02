package sqlstores

import (
	"context"
	"database/sql"
	"errors"
	"io"

	"github.com/blobcache/bpm/internal/dbutil"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/jmoiron/sqlx"
	"github.com/owlmessenger/owl/pkg/migrations"
)

func Migration(x *migrations.State) *migrations.State {
	return x.
		ApplyStmt(`CREATE TABLE blobs (
		id BLOB,
		data BLOB NOT NULL,
		PRIMARY KEY(id)
	);`).
		ApplyStmt(`CREATE TABLE stores (
		id INTEGER PRIMARY KEY
	);`).
		ApplyStmt(`CREATE TABLE store_blobs (
		store_id INTEGER,
		blob_id BLOB,
		FOREIGN KEY(store_id) REFERENCES store_id,
		FOREIGN KEY(blob_id) REFERENCES blobs(id),
		PRIMARY KEY(store_id, blob_id)
	);`)
}

// CreateStore allocates a new store ID which wil not be reused
func CreateStore(tx *sqlx.Tx) (ret uint64, err error) {
	err = tx.Get(&ret, `INSERT INTO stores VALUES (NULL) RETURNING id`)
	return ret, err
}

// DropStore deletes a store and any blobs not included in another store.
func DropStore(tx *sqlx.Tx, storeID int) error {
	if _, err := tx.Exec(`DELETE FROM stores WHERE id = ?`, storeID); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM store_blobs WHERE store_id = ?`, storeID); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM blobs WHERE id NOT IN (
		SELECT blob_id FROM store_blobs
	)`); err != nil {
		return err
	}
	return nil
}

type txStore struct {
	tx      *sqlx.Tx
	intID   uint64
	hf      cadata.HashFunc
	maxSize int
}

func NewTxStore(tx *sqlx.Tx, hf cadata.HashFunc, maxSize int, intID uint64) *txStore {
	return &txStore{tx: tx, hf: hf, intID: intID}
}

func (s *txStore) Post(ctx context.Context, data []byte) (cadata.ID, error) {
	if len(data) > s.MaxSize() {
		return cadata.ID{}, cadata.ErrTooLarge
	}
	id := s.Hash(data)
	if _, err := s.tx.Exec(`INSERT INTO blobs (id, data)
		VALUES (?, ?) ON CONFLICT DO NOTHING`, id[:], data); err != nil {
		return cadata.ID{}, err
	}
	if err := s.add(id); err != nil {
		return cadata.ID{}, err
	}
	return id, nil
}

func (s *txStore) Get(ctx context.Context, id cadata.ID, buf []byte) (int, error) {
	var data []byte
	if err := s.tx.Get(&data, `SELECT blobs.data FROM store_blobs JOIN blobs ON blob_id = blobs.id
		WHERE store_id = ? AND blob_id = ?
	`, s.intID, id[:]); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			err = cadata.ErrNotFound
		}
		return 0, err
	}
	if len(data) > len(buf) {
		return 0, io.ErrShortBuffer
	}
	return copy(buf, data), nil
}

func (s *txStore) Add(ctx context.Context, id cadata.ID) error {
	count, err := s.count(id)
	if err != nil {
		return err
	}
	if count > 0 {
		return s.add(id)
	} else {
		return cadata.ErrNotFound
	}
}

func (s *txStore) add(id cadata.ID) error {
	_, err := s.tx.Exec(`INSERT INTO store_blobs (store_id, blob_id)
		VALUES (?, ?) ON CONFLICT DO NOTHING`, s.intID, id[:])
	return err
}

func (s *txStore) Delete(ctx context.Context, id cadata.ID) error {
	if _, err := s.tx.Exec(`DELETE FROM store_blobs WHERE store_id = ? AND id = ?`, s.intID, id[:]); err != nil {
		return err
	}
	if count, err := s.count(id); err != nil {
		return err
	} else if count < 1 {
		if _, err := s.tx.Exec(`DELETE FROM blobs WHERE id = ?`, id[:]); err != nil {
			return err
		}
	}
	return nil
}

func (s *txStore) List(ctx context.Context, span cadata.Span, ids []cadata.ID) (int, error) {
	begin := cadata.BeginFromSpan(span)
	rows, err := s.tx.Query(`SELECT blob_id FROM store_blobs
		WHERE store_id = ? AND blob_id >= ?
		LIMIT ?
	`, s.intID, begin[:], len(ids))
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var n int
	for rows.Next() && n < len(ids) {
		var buf []byte
		if err := rows.Scan(&buf); err != nil {
			return 0, err
		}
		ids[n] = cadata.IDFromBytes(buf)
		n++
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	return n, nil
}

func (s *txStore) MaxSize() int {
	return s.maxSize
}

func (s *txStore) Hash(x []byte) cadata.ID {
	return s.hf(x)
}

func (s *txStore) count(id cadata.ID) (count int, err error) {
	err = s.tx.Get(&count, `SELECT count(distinct store_id) FROM store_blobs WHERE blob_id = ?`, id[:])
	return count, err
}

type store struct {
	db      *sqlx.DB
	hf      cadata.HashFunc
	maxSize int
	intID   uint64
}

func NewStore(db *sqlx.DB, hf cadata.HashFunc, maxSize int, intID uint64) *store {
	return &store{db: db, hf: hf, maxSize: maxSize, intID: intID}
}

func (s *store) Post(ctx context.Context, data []byte) (cadata.ID, error) {
	return dbutil.DoTx1(ctx, s.db, func(tx *sqlx.Tx) (cadata.ID, error) {
		s2 := s.txStore(tx)
		return s2.Post(ctx, data)
	})
}

func (s *store) Get(ctx context.Context, id cadata.ID, buf []byte) (int, error) {
	return dbutil.DoTx1(ctx, s.db, func(tx *sqlx.Tx) (int, error) {
		s2 := s.txStore(tx)
		return s2.Get(ctx, id, buf)
	})
}

func (s *store) Add(ctx context.Context, id cadata.ID) error {
	return dbutil.DoTx(ctx, s.db, func(tx *sqlx.Tx) error {
		s2 := s.txStore(tx)
		return s2.Add(ctx, id)
	})
}

func (s *store) Delete(ctx context.Context, id cadata.ID) error {
	return dbutil.DoTx(ctx, s.db, func(tx *sqlx.Tx) error {
		s2 := s.txStore(tx)
		return s2.Delete(ctx, id)
	})
}

func (s *store) List(ctx context.Context, span cadata.Span, ids []cadata.ID) (int, error) {
	return dbutil.DoTx1(ctx, s.db, func(tx *sqlx.Tx) (int, error) {
		s2 := s.txStore(tx)
		return s2.List(ctx, span, ids)
	})
}

func (s *store) MaxSize() int {
	s2 := s.txStore(nil)
	return s2.MaxSize()
}

func (s *store) Hash(x []byte) cadata.ID {
	s2 := s.txStore(nil)
	return s2.Hash(x)
}

func (s *store) txStore(tx *sqlx.Tx) txStore {
	return txStore{tx: tx, hf: s.hf, maxSize: s.maxSize, intID: s.intID}
}
