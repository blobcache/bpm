package sources

import (
	"context"
	"errors"
	"strings"

	"github.com/blobcache/glfs"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-state/streams"

	"github.com/blobcache/bpm/bpmmd"
)

// Source is an external Source
type Source interface {
	// Fetch returns an iterator for all the assets in the source
	Fetch(ctx context.Context) (AssetIterator, error)
	Pull(ctx context.Context, op *glfs.Operator, s cadata.Store, id string) (*glfs.Ref, error)
}

type RemoteAsset struct {
	ID     string
	Labels bpmmd.LabelSet
}

// AssetIterator
type AssetIterator = streams.Iterator[RemoteAsset]

type URL struct {
	Scheme string
	Path   string
}

func (u URL) String() string {
	sch := u.Scheme
	if sch == "" {
		sch = "no-scheme"
	}
	return u.Scheme + ":" + u.Path
}

func ParseURL(x string) (*URL, error) {
	if !strings.Contains(x, ":") {
		return nil, errors.New("source url must contain ':' ")
	}
	scheme, path := splitScheme(x)
	return &URL{Scheme: scheme, Path: path}, nil
}

func splitScheme(x string) (scheme string, rest string) {
	parts := strings.SplitN(x, ":", 2)
	switch len(parts) {
	case 1:
		return parts[0], ""
	case 2:
		return parts[0], parts[1]
	default:
		return "", ""
	}
}
