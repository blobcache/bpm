package streams2

import (
	"context"

	"github.com/brendoncarroll/go-state/streams"
	"github.com/jmoiron/sqlx"
)

type SQLIterator[T any] struct {
	rows *sqlx.Rows
}

func NewSQLIterator[T any](rows *sqlx.Rows) *SQLIterator[T] {
	return &SQLIterator[T]{
		rows: rows,
	}
}

func (it *SQLIterator[T]) Next(ctx context.Context, dst *T) error {
	if !it.rows.Next() {
		if err := it.rows.Err(); err != nil {
			return err
		}
		return streams.EOS()
	}
	if err := it.rows.Scan(dst); err != nil {
		return err
	}
	return it.rows.Err()
}
