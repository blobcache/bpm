package streams2

import (
	"context"

	"github.com/brendoncarroll/go-state/streams"
	"golang.org/x/sync/errgroup"
)

type concat[T any] []streams.Iterator[T]

func (it *concat[T]) Next(ctx context.Context, dst *T) error {
	if len(*it) == 0 {
		return streams.EOS()
	}
	if err := (*it)[0].Next(ctx, dst); err != nil {
		if streams.IsEOS(err) {
			*it = (*it)[1:]
			return it.Next(ctx, dst)
		}
		return err
	}
	return nil
}

func Concat[T any](its ...streams.Iterator[T]) streams.Iterator[T] {
	c := concat[T](its)
	return &c
}

// ForEachBuf is a buffered for each, it will read from the iterator in a separate goroutine to a channel.
func ForEachBuf[T any](ctx context.Context, it streams.Iterator[T], n int, fn func(*T) error) error {
	out := make(chan T, n)
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		defer close(out)
		return streams.LoadChan(ctx, it, out)
	})
	eg.Go(func() error {
		for x := range out {
			if err := fn(&x); err != nil {
				return err
			}
		}
		return nil
	})
	return eg.Wait()
}
