package bpmcmd

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/brendoncarroll/stdctx/logctx"
	"github.com/spf13/cobra"

	"github.com/blobcache/bpm"
	"github.com/blobcache/bpm/sources"
)

var (
	repo *bpm.Repo
)

// NewCmd creates a new root command
func NewCmd(ctx context.Context) *cobra.Command {
	c := &cobra.Command{
		Use:   "bpm",
		Short: "bpm is a package manager",
	}
	for _, child := range []*cobra.Command{
		// repo
		newInitCmd(ctx),
		newStatusCmd(ctx),

		newFetchCmd(ctx),
		newFetchAllCmd(ctx),
		newSearchCmd(ctx),
		newInstallCmd(ctx),
		newGetCmd(ctx),

		// type specific
		newAssetCmd(ctx),
		newSnapshotCmd(ctx),
	} {
		c.AddCommand(child)
	}
	return c
}

func newInitCmd(ctx context.Context) *cobra.Command {
	return &cobra.Command{
		Use:   "init",
		Short: "initializes a repository in the current directory",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var p string
			if len(args) > 0 {
				var err error
				p, err = filepath.Abs(args[0])
				if err != nil {
					return err
				}
			} else {
				var err error
				p, err = os.Getwd()
				if err != nil {
					return err
				}
			}
			return bpm.Init(ctx, p)
		},
	}
}

func newStatusCmd(ctx context.Context) *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "prints status information",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			p := getRepoPath()
			return loadRepo(ctx, p)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			bw := bufio.NewWriter(cmd.OutOrStdout())
			fmt.Fprintln(bw, repo)
			return bw.Flush()
		},
	}
}

func newGetCmd(ctx context.Context) *cobra.Command {
	return &cobra.Command{
		Use:   "get <source> <id>",
		Short: "get gets an asset from a source by id",
		Args:  cobra.ExactArgs(2),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			p := getRepoPath()
			return loadRepo(ctx, p)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			sourceURL, err := sources.ParseURL(args[0])
			if err != nil {
				return err
			}
			idstr := args[1]
			aid, err := repo.Pull(ctx, *sourceURL, idstr)
			if err != nil {
				return err
			}
			_, err = fmt.Fprintln(cmd.OutOrStdout(), aid)
			return err
		},
	}
}

func loadRepo(ctx context.Context, p string) error {
	r, err := bpm.Open(p)
	if err != nil {
		return err
	}
	logctx.Infof(ctx, "loaded repo at %s", p)
	repo = r
	return nil
}

func getRepoPath() string {
	p, ok := os.LookupEnv("BPM_PATH")
	if ok {
		return p
	}
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "./" // current directory
	}
	return filepath.Join(homeDir, "pkg")
}
