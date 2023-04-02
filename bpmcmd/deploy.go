package bpmcmd

import (
	"bufio"
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/spf13/cobra"
)

func newDeployCmd(ctx context.Context) *cobra.Command {
	listCmd := &cobra.Command{
		Use:   "list",
		Short: "prints deployments to stdout",
		RunE: func(cmd *cobra.Command, args []string) error {
			ds, err := repo.ListDeploysFull(ctx)
			if err != nil {
				return err
			}
			bufw := bufio.NewWriter(cmd.OutOrStdout())
			fmtStr := "%1s %-6v %-20v\n"
			fmt.Fprintf(bufw, fmtStr, " ", "ID", "CREATED AT")
			for _, d := range ds {
				activeStr := " "
				if d.IsActive() {
					activeStr = "*"
				}
				fmt.Fprintf(bufw, fmtStr, activeStr, d.ID, d.CreatedAt.Format(time.DateTime))
			}
			return bufw.Flush()
		},
	}
	createCmd := &cobra.Command{
		Use:   "create",
		Short: "create a new deployment by name",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			path := args[0]
			assetID, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				return err
			}

			current, err := repo.GetDeploy(ctx, 0)
			if err != nil {
				return err
			}
			assets := map[string]uint64{}
			if current != nil {
				for p, a := range current.Assets {
					assets[p] = a.ID
				}
			}
			assets[path] = assetID

			did, err := repo.Deploy(ctx, assets)
			if err != nil {
				return err
			}
			_, err = fmt.Fprintln(cmd.OutOrStdout(), did)
			return err
		},
	}
	c := &cobra.Command{
		Use:   "deploy",
		Short: "manage deployments",
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
