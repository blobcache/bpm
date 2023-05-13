package streams2

import (
	"context"

	"github.com/brendoncarroll/go-state/streams"
)

type Filter[T any] struct {
	inner streams.Iterator[T]
	pred  func(*T) bool
}

// NewFilter creates a new iterator which filters x using pred.
// The new iterator will only emit an element x for which pred(x) is true.
func NewFilter[T any](x streams.Iterator[T], pred func(*T) bool) Filter[T] {
	return Filter[T]{inner: x, pred: pred}
}

func (it Filter[T]) Next(ctx context.Context, dst *T) error {
	for {
		if err := it.inner.Next(ctx, dst); err != nil {
			return err
		}
		if it.pred(dst) {
			return nil
		}
	}
}
