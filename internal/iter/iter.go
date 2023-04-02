package iter

import (
	"context"
	"errors"

	"golang.org/x/sync/errgroup"
)

var eos = errors.New("end of stream")

// EOS signals the end of the stream
func EOS() error {
	return eos
}

func IsEOS(err error) bool {
	return errors.Is(err, EOS())
}

type Iterator[T any] interface {
	Next(ctx context.Context, x *T) error
}

type Peekable[T any] interface {
	Iterator[T]
	Peek(ctx context.Context, x *T) error
}

type Seekable[T any] interface {
	Iterator[T]
	// Seek advances the iterator.  After Seek returns all elements emitted will be >= gteq
	Seek(ctx context.Context, gteq T) error
}

type concat[T any] []Iterator[T]

func (it *concat[T]) Next(ctx context.Context, dst *T) error {
	if len(*it) == 0 {
		return EOS()
	}
	if err := (*it)[0].Next(ctx, dst); err != nil {
		if IsEOS(err) {
			*it = (*it)[1:]
			return it.Next(ctx, dst)
		}
		return err
	}
	return nil
}

func Concat[T any](its ...Iterator[T]) Iterator[T] {
	c := concat[T](its)
	return &c
}

func ForEach[T any](ctx context.Context, it Iterator[T], fn func(*T) error) error {
	var x T
	for {
		if err := it.Next(ctx, &x); err != nil {
			if IsEOS(err) {
				break
			}
			return err
		}
		if err := fn(&x); err != nil {
			return err
		}
	}
	return nil
}

// ForEachBuf is a buffered for each, it will read from the iterator in a separate goroutine to a channel.
func ForEachBuf[T any](ctx context.Context, it Iterator[T], n int, fn func(*T) error) error {
	out := make(chan T, n)
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		defer close(out)
		return LoadChan(ctx, out, it)
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

// LoadChan reads from it and write to out.
// LoadChan returns an error if ctx is cancelled or if the iterator returns an error.
// LoadChan does not close the channel.
func LoadChan[T any](ctx context.Context, out chan<- T, it Iterator[T]) error {
	for {
		var x T
		if err := it.Next(ctx, &x); err != nil {
			if IsEOS(err) {
				break
			}
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- x:
		}
	}
	return nil
}
