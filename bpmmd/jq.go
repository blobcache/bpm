package bpmmd

import (
	"context"
	"fmt"

	"github.com/brendoncarroll/go-exp/streams"
	"github.com/itchyny/gojq"
	"golang.org/x/exp/maps"
)

type JQFilter[T any] struct {
	x    streams.Iterator[T]
	code *gojq.Code
	fn   func(T) LabelSet

	obj map[string]any
}

func NewJQFilter[T any](x streams.Iterator[T], code *gojq.Code, fn func(T) LabelSet) *JQFilter[T] {
	return &JQFilter[T]{
		x:    x,
		code: code,
		fn:   fn,

		obj: make(map[string]any),
	}
}

func (it *JQFilter[T]) Next(ctx context.Context, dst *T) error {
	for {
		if err := it.x.Next(ctx, dst); err != nil {
			return err
		}
		ls := it.fn(*dst)
		maps.Clear(it.obj)
		for k, v := range ls {
			it.obj[k] = v
		}
		jqit := it.code.Run(it.obj)

		out, ok := jqit.Next()
		if !ok {
			return fmt.Errorf("jq iterator did not return any values")
		}
		if err, ok := out.(error); ok {
			return err
		}
		if out, ok := jqit.Next(); ok {
			return fmt.Errorf("jq iterator returned second value: %v", out)
		}
		allow, ok := out.(bool)
		if !ok {
			return fmt.Errorf("jq expression must return boolean. instead got: %v", out)
		}
		if allow {
			break
		}
	}
	return nil
}
