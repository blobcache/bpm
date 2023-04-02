package sources

import (
	"context"

	"github.com/blobcache/glfs"
	"github.com/brendoncarroll/go-state/cadata"

	"github.com/blobcache/bpm/bpmmd"
	"github.com/blobcache/bpm/internal/iter"
)

// Source is an external Source
type Source interface {
	Search(ctx context.Context, q bpmmd.Query) (ResultIterator, error)

	PullAsset(ctx context.Context, op *glfs.Operator, s cadata.Store, id string) (*glfs.Ref, error)
}

type Result struct {
	ID     string
	Labels bpmmd.LabelSet
}

// ResultIterator
type ResultIterator = iter.Iterator[Result]
