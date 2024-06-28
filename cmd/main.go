package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/databendcloud/db-archiver/config"
	"github.com/databendcloud/db-archiver/ingester"
	"github.com/databendcloud/db-archiver/source"
	"github.com/databendcloud/db-archiver/worker"
)

func main() {
	start := fmt.Sprintf("start time: %s", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Println(start)
	startTime := time.Now()
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		sigch := make(chan os.Signal, 1)
		signal.Notify(sigch, syscall.SIGQUIT, syscall.SIGTERM)
		<-sigch
		cancel()
	}()

	cfg := parseConfigWithFile()
	ig := ingester.NewDatabendIngester(cfg)
	src, err := source.NewSource(cfg)
	if err != nil {
		panic(err)
	}
	wg := sync.WaitGroup{}
	wg.Add(1)
	w := worker.NewWorker(cfg, fmt.Sprintf("worker"), ig, src)
	go func() {
		w.Run(ctx)
		wg.Done()
	}()
	wg.Wait()
	endTime := fmt.Sprintf("end time: %s", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Println(endTime)
	fmt.Println(fmt.Sprintf("total time: %s", time.Since(startTime)))
}

func parseConfigWithFile() *config.Config {
	cfg, err := config.LoadConfig()
	if err != nil {
		panic(err)
	}
	return cfg
}
