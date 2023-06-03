package streams2

import (
	"context"

	"github.com/brendoncarroll/go-exp/streams"
	"golang.org/x/sync/errgroup"
)

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
