package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"samplechain/internal/app"
)

func main() {
	address := flag.String("address", ":8080", "HTTP 监听地址")
	ledgerPath := flag.String("ledger", "samplechain.json", "JSON 账本路径")
	flag.Parse()

	application, err := app.New(app.Config{Address: *address, LedgerPath: *ledgerPath})
	if err != nil {
		fmt.Fprintln(os.Stderr, "服务配置失败:", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := application.Run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "服务运行失败:", err)
		os.Exit(1)
	}
}
