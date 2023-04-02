package main

import (
	"context"
	"log"

	"github.com/brendoncarroll/stdctx/logctx"
	"go.uber.org/zap"

	"github.com/blobcache/bpm/bpmcmd"
)

func main() {
	ctx := context.Background()
	l, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	ctx = logctx.NewContext(ctx, l)
	cmd := bpmcmd.NewCmd(ctx)
	if err := cmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
