package bpmcmd

import (
	"bufio"
	"context"
	"errors"
	"fmt"

	"github.com/brendoncarroll/go-state"
	"github.com/brendoncarroll/go-state/posixfs"
	"github.com/spf13/cobra"
)

func newAssetCmd(ctx context.Context) *cobra.Command {
	listCmd := &cobra.Command{
		Use:   "list",
		Short: "lists assets",
		RunE: func(cmd *cobra.Command, args []string) error {
			as, err := repo.ListAssetsFull(ctx, state.TotalSpan[uint64](), 0)
			if err != nil {
				return err
			}
			bufw := bufio.NewWriter(cmd.OutOrStdout())
			fmtStr := "%-8v %-40v %-6v %-12v %v\n"
			fmt.Fprintf(bufw, fmtStr, "ID", "UPSTREAM", "TYPE", "SIZE", "CID")
			for _, a := range as {
				var ustr string
				if a.Upstream != nil {
					ustr = a.Upstream.String()
				}
				fmt.Fprintf(bufw, fmtStr, a.ID, ustr, a.Root.Type, a.Root.Size, a.Root.CID.String())
			}
			return bufw.Flush()
		},
	}

	var importPath string
	createCmd := &cobra.Command{
		Use:   "create",
		Short: "create a new asset",
		RunE: func(cmd *cobra.Command, args []string) error {
			if importPath == "" {
				return errors.New("must provide path to object")
			}
			fs := posixfs.NewOSFS()
			aid, err := repo.CreateAssetFS(ctx, fs, importPath)
			if err != nil {
				return err
			}
			_, err = fmt.Fprintln(cmd.OutOrStdout(), aid)
			return err
		},
	}
	createCmd.PersistentFlags().StringVarP(&importPath, "", "f", "", "-f=<path>")

	c := &cobra.Command{
		Use: "asset",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			p := getRepoPath()
			return loadRepo(ctx, p)
		},
	}
	for _, child := range []*cobra.Command{
		listCmd,
		createCmd,
	} {
		c.AddCommand(child)
	}
	return c
}
