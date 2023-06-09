package bpm

import (
	"context"
	"database/sql"
	"errors"
	"path"
	"strings"
	"time"

	"github.com/brendoncarroll/go-exp/streams"
	"github.com/brendoncarroll/stdctx/logctx"
	"github.com/itchyny/gojq"
	"github.com/jmoiron/sqlx"
	"golang.org/x/sync/errgroup"

	"github.com/blobcache/bpm/bpmmd"
	"github.com/blobcache/bpm/internal/dbutil"
	"github.com/blobcache/bpm/internal/sqlstores"
	"github.com/blobcache/bpm/sources"
	"github.com/blobcache/bpm/sources/github"
	"github.com/blobcache/bpm/sources/httpscrape"
)

// MakeSource creates a new source from a URL
func MakeSource(u sources.URL) (sources.Source, error) {
	switch u.Scheme {
	case "github":
		parts := strings.SplitN(u.Path, "/", 2)
		return github.NewGitHubSource(parts[0], parts[1]), nil
	case "http":
		s, err := httpscrape.NewHTTPScraper(u.Path)
		return s, err
	default:
		return nil, errors.New("unrecognized URL scheme")
	}
}

// UpstreamURL uniquely identifies a remote asset
type UpstreamURL struct {
	sources.URL
	ID string `json:"id"`
}

func (u UpstreamURL) String() string {
	return path.Join(u.URL.String(), u.ID)
}

// Fetch creates metadata-only assets for all of assets in the source.
func (r *Repo) Fetch(ctx context.Context, srcURL sources.URL) error {
	src, err := MakeSource(srcURL)
	if err != nil {
		return err
	}
	it, err := src.Fetch(ctx)
	if err != nil {
		return err
	}

	eg, ctx := errgroup.WithContext(ctx)
	results := make(chan sources.RemoteAsset)
	eg.Go(func() error {
		defer close(results)
		return streams.LoadChan(ctx, it, results)
	})
	eg.Go(func() error {
		const batchSize = 1000
		const timeout = 100 * time.Millisecond
		it := streams.NewBatcher[sources.RemoteAsset](streams.Chan[sources.RemoteAsset](results), batchSize, timeout)
		return streams.ForEach[[]sources.RemoteAsset](ctx, it, func(xs []sources.RemoteAsset) error {
			return dbutil.DoTx(ctx, r.db, func(tx *sqlx.Tx) error {
				for _, x := range xs {
					assetID, err := getOrCreateUpstream(tx, srcURL.Scheme, srcURL.Path, x.ID)
					if err != nil {
						return err
					}
					if err := putLabelSet(tx, assetID, x.Labels); err != nil {
						return err
					}
				}
				return nil
			})
		})
	})
	return eg.Wait()
}

func (r *Repo) FetchAll(ctx context.Context) error {
	var rows []struct {
		Scheme string `db:"scheme"`
		Path   string `db:"path"`
	}
	if err := r.db.SelectContext(ctx, &rows, `SELECT distinct scheme, path FROM upstreams`); err != nil {
		return err
	}
	var eg errgroup.Group
	eg.SetLimit(10)
	for _, row := range rows {
		u := sources.URL{
			Scheme: row.Scheme,
			Path:   row.Path,
		}
		logctx.Infof(ctx, "fetching asset metadata from %v", u)
		eg.Go(func() error {
			return r.Fetch(ctx, u)
		})
	}
	return eg.Wait()
}

// Pull pulls the content for an asset from source
func (r *Repo) Pull(ctx context.Context, u sources.URL, idstr string) (uint64, error) {
	src, err := MakeSource(u)
	if err != nil {
		return 0, err
	}
	aid, err := dbutil.DoTx1(ctx, r.db, func(tx *sqlx.Tx) (uint64, error) {
		return getOrCreateUpstream(tx, u.Scheme, u.Path, idstr)
	})
	if err != nil {
		return 0, err
	}
	sid, err := dbutil.DoTx1(ctx, r.db, func(tx *sqlx.Tx) (uint64, error) { return getAssetStore(tx, aid) })
	if err != nil {
		return 0, err
	}
	s := sqlstores.NewStore(r.db, Hash, MaxBlobSize, sid)
	ref, err := src.Pull(ctx, &r.glfsOp, s, idstr)
	if err != nil {
		return 0, err
	}
	if err := dbutil.DoTx(ctx, r.db, func(tx *sqlx.Tx) error { return putAssetRef(tx, aid, *ref) }); err != nil {
		return 0, err
	}
	return aid, nil
}

// Search searches locally cached remote assets for a source.
// To search assets originating locally pass nil for srcURL
func (r *Repo) ListAssetsBySource(ctx context.Context, srcURL *sources.URL, code *gojq.Code) ([]Asset, error) {
	qstr := `SELECT assets.id FROM assets
		JOIN asset_labels ON asset_labels.asset_id = assets.id
		JOIN upstreams ON upstreams.asset_id = assets.id
	`
	var args []any
	if srcURL != nil {
		qstr += " WHERE upstreams.scheme = ? AND upstreams.path = ?"
		args = append(args, srcURL.Scheme, srcURL.Path)
	} else {
		qstr += " WHERE assets.upstream_id IS NULL"
	}
	var fromDB []uint64
	if err := r.db.SelectContext(ctx, &fromDB, qstr, args...); err != nil {
		return nil, err
	}
	eg, ctx := errgroup.WithContext(ctx)
	unfiltered := make(chan Asset)
	eg.Go(func() error {
		defer close(unfiltered)
		for _, aid := range fromDB {
			a, err := r.GetAsset(ctx, aid)
			if err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case unfiltered <- a:
			}
		}
		return nil
	})
	var ret []Asset
	eg.Go(func() error {
		in := streams.Chan[Asset](unfiltered)
		it := bpmmd.NewJQFilter[Asset](in, code, func(x Asset) bpmmd.LabelSet {
			return x.Labels
		})
		var err error
		ret, err = streams.Collect[Asset](ctx, it, 1e9)
		return err
	})
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return ret, nil
}

// getOrCreateUpstream returns the asset for a given upstream
func getOrCreateUpstream(tx *sqlx.Tx, scheme, path, remoteID string) (ret uint64, _ error) {
	err := tx.Get(&ret, `SELECT asset_id FROM upstreams WHERE scheme = ? AND path = ? AND remote_id = ?`, scheme, path, remoteID)
	if errors.Is(err, sql.ErrNoRows) {
		sid, err := sqlstores.CreateStore(tx)
		if err != nil {
			return 0, err
		}
		aid, err := createAsset(tx, sid)
		if err != nil {
			return 0, err
		}
		if _, err := tx.Exec(`INSERT INTO upstreams (scheme, path, remote_id, asset_id) VALUES (?, ?, ?, ?)`, scheme, path, remoteID, aid); err != nil {
			return 0, err
		}
		return aid, nil
	}
	return ret, err
}

func lookupUpstream(tx *sqlx.Tx, aid uint64) (*UpstreamURL, error) {
	var row struct {
		Scheme   string `db:"scheme"`
		Path     string `db:"path"`
		RemoteID string `db:"remote_id"`
	}
	if err := tx.Get(&row, `SELECT scheme, path, remote_id FROM upstreams WHERE asset_id = ?`, aid); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &UpstreamURL{
		URL: sources.URL{
			Scheme: row.Scheme,
			Path:   row.Path,
		},
		ID: row.RemoteID,
	}, nil
}
