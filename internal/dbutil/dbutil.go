package dbutil

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/brendoncarroll/go-state"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

func NewTestDB(t testing.TB) *sqlx.DB {
	db, err := OpenDB(":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func OpenDB(dsn string) (*sqlx.DB, error) {
	db, err := sqlx.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	return db, nil
}

func DoTx(ctx context.Context, db *sqlx.DB, fn func(tx *sqlx.Tx) error) error {
	tx, err := db.BeginTxx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func DoTx1[T any](ctx context.Context, db *sqlx.DB, fn func(tx *sqlx.Tx) (T, error)) (T, error) {
	var ret, zero T
	err := DoTx(ctx, db, func(tx *sqlx.Tx) error {
		ret = zero
		var err error
		ret, err = fn(tx)
		return err
	})
	return ret, err
}

func DoTx2[A, B any](ctx context.Context, db *sqlx.DB, fn func(tx *sqlx.Tx) (A, B, error)) (A, B, error) {
	var a, zeroA A
	var b, zeroB B
	err := DoTx(ctx, db, func(tx *sqlx.Tx) error {
		a, b = zeroA, zeroB
		var err error
		a, b, err = fn(tx)
		return err
	})
	return a, b, err
}

type ListParams[T any] struct {
	Span state.Span[T]

	MaxLimit     int
	DefaultLimit int
	Limit        int
}

func List[T any](ctx context.Context, db *sqlx.DB, query, primaryKey string, p ListParams[T]) ([]T, error) {
	if p.Limit > p.MaxLimit {
		p.Limit = p.MaxLimit
	}
	if p.Limit <= 0 {
		p.Limit = p.DefaultLimit
	}
	query += fmt.Sprintf(" ORDER BY %s", primaryKey)
	var args []any
	if lower, ok := p.Span.LowerBound(); ok {
		query += fmt.Sprintf(" WHERE %s >= ?", primaryKey)
		args = append(args, lower)
	}
	if upper, ok := p.Span.UpperBound(); ok {
		if len(args) > 0 {
			query += fmt.Sprintf(" AND %s < ?", primaryKey)
		} else {
			query += fmt.Sprintf(" WHERE %s < ?", primaryKey)
		}
		args = append(args, upper)
	}
	return DoTx1(ctx, db, func(tx *sqlx.Tx) (ret []T, _ error) {
		err := tx.Select(&ret, query, args...)
		return ret, err
	})
}
