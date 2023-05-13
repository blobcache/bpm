package bpm

import (
	"time"

	"github.com/blobcache/glfs"
	"github.com/blobcache/webref"
	"github.com/brendoncarroll/go-state"
	"github.com/brendoncarroll/go-state/cadata"
	"lukechampine.com/blake3"

	"github.com/blobcache/bpm/bpmmd"
	"github.com/blobcache/bpm/sources"
)

const MaxBlobSize = 1 << 21

// Hash is the hash function used by BPM
func Hash(x []byte) cadata.ID {
	return blake3.Sum256(x)
}

type (
	Label    = bpmmd.Label
	LabelSet = bpmmd.LabelSet
)

type Asset struct {
	ID       uint64       `json:"id"`
	Labels   LabelSet     `json:"labels"`
	Root     glfs.Ref     `json:"root"`
	Upstream *UpstreamURL `json:"upstream"`
}

func (a Asset) IsLocal() bool {
	return a.Upstream == nil
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

type Manifest map[string]DeploySpec

type DeploySpec struct {
	Source sources.URL `json:"source"`
	Query  string      `json:"query"`
}
