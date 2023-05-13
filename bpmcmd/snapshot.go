package bpmcmd

import (
	"bufio"
	"context"
	"fmt"

	"github.com/spf13/cobra"
)

func newSnapshotCmd(ctx context.Context) *cobra.Command {
	listCmd := &cobra.Command{
		Use:   "list",
		Short: "prints snapshots to stdout",
		RunE: func(cmd *cobra.Command, args []string) error {
			ss, err := repo.ListSnapshotsFull(ctx)
			if err != nil {
				return err
			}
			current, err := repo.GetCurrent(ctx)
			if err != nil {
				return err
			}
			bufw := bufio.NewWriter(cmd.OutOrStdout())
			fmtStr := "%1s %-45v %-20v\n"
			fmt.Fprintf(bufw, fmtStr, " ", "ID", "ASSET COUNT")
			for _, s := range ss {
				activeStr := " "
				if s.ID == current.Snapshot {
					activeStr = "*"
				}
				fmt.Fprintf(bufw, fmtStr, activeStr, s.ID, len(s.TLDs))
			}
			return bufw.Flush()
		},
	}
	c := &cobra.Command{
		Use:   "snapshot",
		Short: "manage snapshot",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			p := getRepoPath()
			return loadRepo(ctx, p)
		},
	}
	for _, child := range []*cobra.Command{
		listCmd,
	} {
		c.AddCommand(child)
	}
	return c
}
