package bpm

import (
	"bytes"
	"context"
	"testing"

	"github.com/brendoncarroll/go-state"
	"github.com/brendoncarroll/go-state/posixfs"
	"github.com/stretchr/testify/require"
)

func TestInitRepo(t *testing.T) {
	newTestRepo(t)
}

func TestAssetCRUD(t *testing.T) {
	r := newTestRepo(t)
	require.Empty(t, listAssets(t, r))

	aid := createAssetTest(t, r, []byte("hello world"))
	require.Equal(t, []uint64{aid}, listAssets(t, r))
}

func TestDeployCRUD(t *testing.T) {
	ctx := context.Background()
	r := newTestRepo(t)
	require.Empty(t, listDeploysTest(t, r))

	aid := createAssetTest(t, r, []byte("hello world"))
	_, err := r.Deploy(ctx, map[string]uint64{
		"hw": aid,
	})
	require.NoError(t, err)
	ds := listDeploysTest(t, r)
	require.NotEmpty(t, ds)
	require.Contains(t, ds[0].Assets, "hw")
}

func newTestRepo(t testing.TB) *Repo {
	ctx := context.Background()
	p := t.TempDir()
	require.NoError(t, Init(ctx, p))
	r, err := Open(p)
	require.NoError(t, err)
	return r
}

func listAssets(t testing.TB, r *Repo) []uint64 {
	ctx := context.Background()
	ids, err := r.ListAssets(ctx, state.TotalSpan[uint64](), 0)
	require.NoError(t, err)
	return ids
}

// createAssetTest creates an asset in a test
func createAssetTest(t testing.TB, r *Repo, data []byte) uint64 {
	ctx := context.Background()
	dirp := t.TempDir()
	fsx := posixfs.NewDirFS(dirp)
	err := posixfs.PutFile(ctx, fsx, "test.txt", 0o644, bytes.NewReader(data))
	require.NoError(t, err)

	aid, err := r.CreateAssetFS(ctx, fsx, "test.txt")
	require.NoError(t, err)
	return aid
}

func listDeploysTest(t testing.TB, r *Repo) []Deploy {
	ds, err := r.ListDeploysFull(context.Background())
	require.NoError(t, err)
	return ds
}
