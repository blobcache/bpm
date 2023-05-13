package bpmcmd

import (
	"bufio"
	"context"
	"fmt"
	"strconv"

	"github.com/blobcache/bpm/sources"
	"github.com/brendoncarroll/stdctx/logctx"
	"github.com/itchyny/gojq"
	"github.com/spf13/cobra"
)

func newSearchCmd(ctx context.Context) *cobra.Command {
	c := &cobra.Command{
		Use:   "search",
		Short: "search a source for a package",
		Args:  cobra.MinimumNArgs(1),
	}
	shouldFetch := c.Flags().Bool("fetch", false, "--fetch")
	c.RunE = func(cmd *cobra.Command, args []string) error {
		if err := loadRepo(ctx, getRepoPath()); err != nil {
			return err
		}
		srcURL, err := sources.ParseURL(args[0])
		if err != nil {
			return err
		}
		if *shouldFetch {
			logctx.Infof(ctx, "fetching...")
			if err := repo.Fetch(ctx, *srcURL); err != nil {
				return err
			}
		}
		// where clause
		jqpred, err := gojq.Parse("true")
		if err != nil {
			return err
		}
		if len(args) > 1 {
			jqpred, err = gojq.Parse(args[1])
			if err != nil {
				return err
			}
		}
		jqcode, err := gojq.Compile(jqpred)
		if err != nil {
			return err
		}

		assets, err := repo.ListAssetsBySource(ctx, srcURL, jqcode)
		if err != nil {
			return err
		}
		bufw := bufio.NewWriter(cmd.OutOrStdout())
		fmtStr := "%-3s %-25s %s\n"
		fmt.Fprintf(bufw, fmtStr, "#", "ID", "LABELS")
		for i, a := range assets {
			idStr := strconv.Itoa(int(a.ID))
			if a.Upstream != nil {
				idStr = a.Upstream.ID
			}
			_, err := fmt.Fprintf(bufw, fmtStr, strconv.Itoa(i), idStr, a.Labels)
			if err != nil {
				return err
			}
		}
		return bufw.Flush()
	}
	return c
}

func newFetchCmd(ctx context.Context) *cobra.Command {
	return &cobra.Command{
		Use:   "fetch",
		Short: "download asset metadata from a source",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			u, err := sources.ParseURL(args[0])
			if err != nil {
				return err
			}
			if err := loadRepo(ctx, getRepoPath()); err != nil {
				return err
			}
			return repo.Fetch(ctx, *u)
		},
	}
}

func newFetchAllCmd(ctx context.Context) *cobra.Command {
	return &cobra.Command{
		Use:   "fetch-all",
		Short: "download asset metadata from a source",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := loadRepo(ctx, getRepoPath()); err != nil {
				return err
			}
			return repo.FetchAll(ctx)
		},
	}
}
