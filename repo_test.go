package bpm

import (
	"bytes"
	"context"
	"strconv"
	"testing"

	"github.com/blobcache/glfs"
	"github.com/brendoncarroll/go-state"
	"github.com/brendoncarroll/go-state/posixfs"
	"github.com/stretchr/testify/require"
)

func TestInitRepo(t *testing.T) {
	newTestRepo(t)
}

func TestAssetCRUD(t *testing.T) {
	r := newTestRepo(t)
	require.Empty(t, mustListAssets(t, r))

	aid := mustCreateAsset(t, r, []byte("hello world"))
	require.Equal(t, []uint64{aid}, mustListAssets(t, r))
}

func TestSnapshotCRUD(t *testing.T) {
	ctx := context.Background()
	r := newTestRepo(t)
	require.Empty(t, mustListSnapshots(t, r))

	aid := mustCreateAsset(t, r, []byte("hello world"))
	a := mustGetAsset(t, r, aid)
	sid, err := r.PostSnapshot(ctx, map[string]glfs.Ref{
		"hw": a.Root,
	})

	require.NoError(t, err)
	require.NotZero(t, sid)
	ss := mustListSnapshots(t, r)
	require.NotEmpty(t, ss)
	require.Contains(t, ss[0].TLDs, "hw")
}

func TestModify(t *testing.T) {
	ctx := context.Background()
	r := newTestRepo(t)

	cs := mustListCommits(t, r)
	require.Len(t, cs, 0)
	aid := mustCreateAsset(t, r, []byte("hello world"))
	a := mustGetAsset(t, r, aid)
	for i := 0; i < 10; i++ {
		_, err := r.Modfiy(ctx, func(tlds map[string]glfs.Ref) error {
			tlds["tld-"+strconv.Itoa(i)] = a.Root
			return nil
		})
		require.NoError(t, err)
	}
	cs = mustListCommits(t, r)
	require.Len(t, cs, 10)
}

func newTestRepo(t testing.TB) *Repo {
	ctx := context.Background()
	p := t.TempDir()
	require.NoError(t, Init(ctx, p))
	r, err := Open(p)
	require.NoError(t, err)
	return r
}

func mustListAssets(t testing.TB, r *Repo) []uint64 {
	ctx := context.Background()
	ids, err := r.ListAssets(ctx, state.TotalSpan[uint64](), 0)
	require.NoError(t, err)
	return ids
}

func mustGetAsset(t testing.TB, r *Repo, id uint64) Asset {
	ctx := context.Background()
	a, err := r.GetAsset(ctx, id)
	require.NoError(t, err)
	return a
}

// createAsset creates an asset in a test
func mustCreateAsset(t testing.TB, r *Repo, data []byte) uint64 {
	ctx := context.Background()
	dirp := t.TempDir()
	fsx := posixfs.NewDirFS(dirp)
	err := posixfs.PutFile(ctx, fsx, "test.txt", 0o644, bytes.NewReader(data))
	require.NoError(t, err)

	aid, err := r.CreateAssetFS(ctx, fsx, "test.txt")
	require.NoError(t, err)
	return aid
}

func mustListSnapshots(t testing.TB, r *Repo) []Snapshot {
	ss, err := r.ListSnapshotsFull(context.Background())
	require.NoError(t, err)
	return ss
}

func mustListCommits(t testing.TB, r *Repo) []Commit {
	ctx := context.Background()
	cs, err := r.ListCommits(ctx)
	require.NoError(t, err)
	return cs
}
