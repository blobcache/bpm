package bpm

import (
	"context"

	"github.com/blobcache/bpm/internal/sqlstores"
	"github.com/jmoiron/sqlx"
	"github.com/owlmessenger/owl/pkg/migrations"
)

var schema = func() *migrations.State {
	x := migrations.InitialState()
	x = sqlstores.Migration(x)
	x = x.ApplyStmt(`CREATE TABLE assets (
		id INTEGER,
		store_id INTEGER REFERENCES stores(id),
		root BLOB,

		PRIMARY KEY(id)
	)`)
	x = x.ApplyStmt(`CREATE INDEX asset_root_idx ON assets (root, id)`)
	x = x.ApplyStmt(`CREATE TABLE asset_labels (
		asset_id INTEGER REFERENCES assets(id), 
		k TEXT,
		v TEXT,
		
		PRIMARY KEY(asset_id, k)
	)`)
	x = x.ApplyStmt(`CREATE INDEX asset_labels_idx_kv ON asset_labels (k, v, asset_id)`)

	x = x.ApplyStmt(`CREATE TABLE upstreams (
		scheme TEXT NOT NULL,
		path TEXT NOT NULL,
		remote_id TEXT NOT NULL,
		asset_id INTEGER NOT NULL REFERENCES assets(id),

		PRIMARY KEY(scheme, path, remote_id)
	)`)
	x = x.ApplyStmt(`CREATE INDEX upstream_idx_asset ON upstreams (asset_id, scheme, path, remote_id)`)

	x = x.ApplyStmt(`CREATE TABLE snapshots (
		id INTEGER NOT NULL,
		cid BLOB NOT NULL,
	
		UNIQUE (cid),
		PRIMARY KEY (id)
	)`)
	x = x.ApplyStmt(`CREATE TABLE snapshot_tlds (
		snapshot_id INTEGER NOT NULL REFERENCES snapshots(id),
		path TEXT NOT NULL,
		root BLOB NOT NULL REFERENCES assets(root),	

		PRIMARY KEY(snapshot_id, path)
	)`)

	x = x.ApplyStmt(`CREATE TABLE commits (
		id INTEGER NOT NULL,
		snapshot_id INTEGER NOT NULL REFERENCES snapshots(id),
		created_at TIMESTAMP NOT NULL,

		PRIMARY KEY(id)
	)`)

	x = x.ApplyStmt(`CREATE TABLE fs_cache (
		path TEXT NOT NULL,
		mtime INTEGER NOT NULL,
		root BLOB NOT NULL,
		PRIMARY KEY(path)
	)`)

	x = x.ApplyStmt(`CREATE TABLE webrefs (
		blob_id BLOB,
		ref BLOB,
		
		PRIMARY KEY(blob_id, ref)
	)`)

	return x
}()

func setupDB(ctx context.Context, db *sqlx.DB) error {
	return migrations.Migrate(ctx, db, schema)
}
