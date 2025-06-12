package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/maki3cat/mkraft"
	"github.com/maki3cat/mkraft/common"
	"go.uber.org/zap"
)

func main() {

	// basics
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	defer logger.Sync()

	// config
	path := "./config/base.yaml"
	pathInArgs := flag.String("c", "", "the path of the config file")
	flag.Parse()
	pathInEnv := os.Getenv("MKRAFT_CONFIG_PATH") // env has higher priority than args

	if pathInEnv != "" {
		path = pathInEnv
	} else if *pathInArgs != "" {
		path = *pathInArgs
	}
	logger.Info("Loading Config from Path", zap.String("path", path))

	cfg, err := common.LoadConfig(path)
	if err != nil {
		panic(err)
	}

	// server start
	server, err := mkraft.NewServer(cfg, logger)
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server.Start(ctx)

	// waiting for close signal
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)
	sig := <-signalChan
	logger.Warn("Received signal", zap.String("signal", sig.String()))
	cancel()
	logger.Info("waiting for server to stop")
	time.Sleep(5 * time.Second)
	logger.Warn("Main exiting")
}
