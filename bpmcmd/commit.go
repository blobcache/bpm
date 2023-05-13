package bpmcmd

import (
	"context"
	"errors"
	"fmt"

	"github.com/blobcache/bpm/sources"
	"github.com/blobcache/glfs"
	"github.com/spf13/cobra"
)

func newInstallCmd(ctx context.Context) *cobra.Command {
	c := &cobra.Command{
		Use:   "install <name> <source> <query>",
		Short: "install takes a source and query, resolves it to a package and then deploys it",
		Args:  cobra.ExactArgs(2),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			p := getRepoPath()
			return loadRepo(ctx, p)
		},
	}
	idstr := c.Flags().String("id", "", "--id=remote-asset-id-1234")
	c.RunE = func(cmd *cobra.Command, args []string) error {
		path := args[0]
		sourceURL, err := sources.ParseURL(args[1])
		if err != nil {
			return err
		}
		if *idstr == "" {
			return errors.New("must provide --id flag")
		}
		assetID, err := repo.Pull(ctx, *sourceURL, *idstr)
		if err != nil {
			return err
		}
		newAsset, err := repo.GetAsset(ctx, assetID)
		if err != nil {
			return err
		}
		commit, err := repo.Modfiy(ctx, func(tlds map[string]glfs.Ref) error {
			tlds[path] = newAsset.Root
			return nil
		})
		if err != nil {
			return err
		}
		fmt.Printf("OK. commit=%d snapshot=%v\n", commit.ID, commit.Snapshot)
		return nil
	}
	return c
}

func newRemoveCmd(ctx context.Context) *cobra.Command {
	return &cobra.Command{
		Use:   "remove <tld>",
		Short: "removes whatever is installed at the directory",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			path := args[0]
			commit, err := repo.Modfiy(ctx, func(tlds map[string]glfs.Ref) error {
				delete(tlds, path)
				return nil
			})
			if err != nil {
				return err
			}
			fmt.Printf("OK. commit=%d snapshot=%v", commit.ID, commit.Snapshot)
			return nil
		},
	}
}
