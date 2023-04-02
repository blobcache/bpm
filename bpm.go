package bpm

import (
	"errors"
	"log"
	"strings"
	"time"

	"github.com/blobcache/glfs"
	"github.com/blobcache/webref"
	"github.com/brendoncarroll/go-state"
	"github.com/brendoncarroll/go-state/cadata"
	"lukechampine.com/blake3"

	"github.com/blobcache/bpm/bpmmd"
	"github.com/blobcache/bpm/sources"
	"github.com/blobcache/bpm/sources/github"
)

const MaxBlobSize = 1 << 21

// Hash is the hash function used by BPM
func Hash(x []byte) cadata.ID {
	return blake3.Sum256(x)
}

type (
	Label    = bpmmd.Label
	LabelSet = bpmmd.LabelSet
	Query    = bpmmd.Query
)

type Asset struct {
	ID     uint64   `json:"id"`
	Labels LabelSet `json:"labels"`
	Root   glfs.Ref `json:"root"`
}

type Deploy struct {
	ID         uint64           `json:"id"`
	Assets     map[string]Asset `json:"assets"`
	CreatedAt  time.Time        `json:"created_at"`
	TeardownAt *time.Time       `json:"teardown_at"`
}

func (d Deploy) IsActive() bool {
	return d.TeardownAt == nil
}

type WebRefStore interface {
	state.Putter[cadata.ID, webref.Ref]
	state.Getter[cadata.ID, webref.Ref]
}

// MakeSource creates a new source from a URL
func MakeSource(x string) (sources.Source, error) {
	sch, rest := splitScheme(x)
	switch sch {
	case "github":
		log.Println("importing from github")
		parts := strings.SplitN(rest, "/", 2)
		return github.NewGitHubSource(parts[0], parts[1]), nil
	default:
		return nil, errors.New("unrecognized URL scheme")
	}
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
