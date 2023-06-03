package streams2

import (
	"context"

	"github.com/brendoncarroll/go-exp/streams"
)

type Mapper[A, B any] struct {
	inner streams.Iterator[A]
	fn    func(A) B

	a A
}

func NewMapper[A, B any](inner streams.Iterator[A], fn func(A) B) streams.Iterator[B] {
	return &Mapper[A, B]{inner: inner, fn: fn}
}

func (m *Mapper[A, B]) Next(ctx context.Context, dst *B) error {
	if err := m.inner.Next(ctx, &m.a); err != nil {
		return err
	}
	*dst = m.fn(m.a)
	return nil
}
