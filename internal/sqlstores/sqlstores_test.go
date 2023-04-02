package sqlstores

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/blobcache/bpm/internal/dbutil"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-state/cadata/storetest"
	"github.com/owlmessenger/owl/pkg/migrations"
	"github.com/stretchr/testify/require"
)

func TestStore(t *testing.T) {
	db := dbutil.NewTestDB(t)
	err := migrations.Migrate(context.TODO(), db, Migration(migrations.InitialState()))
	require.NoError(t, err)

	var n int32
	storetest.TestStore(t, func(t testing.TB) cadata.Store {
		i := atomic.AddInt32(&n, 1)
		s := NewStore(db, cadata.DefaultHash, 1<<21, uint64(i))
		return s
	})
}
