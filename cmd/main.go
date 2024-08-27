package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/databendcloud/db-archiver/config"
	"github.com/databendcloud/db-archiver/ingester"
	"github.com/databendcloud/db-archiver/source"
	"github.com/databendcloud/db-archiver/worker"
)

func main() {
	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()
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

	configFile := flag.String("f", "", "Path to the configuration file")
	flag.Parse()

	if *configFile == "" {
		*configFile = "config/conf.json"
		if _, err := os.Stat(*configFile); os.IsNotExist(err) {
			fmt.Printf("json config file does not exist, you can use -f to specify it.Example: ./dbarchiver -f conf.json \n")
			os.Exit(1)
		}
	}
	cfg := parseConfigWithFile(*configFile)
	ig := ingester.NewDatabendIngester(cfg)
	src, err := source.NewSource(cfg)
	if err != nil {
		panic(err)
	}

	dbTables := make(map[string][]string)
	if len(cfg.SourceDbTables) != 0 {
		dbTables, err = src.GetDbTablesAccordingToSourceDbTables()
		if err != nil {
			panic(err)
		}
	} else {
		dbs, err := src.GetDatabasesAccordingToSourceDbRegex(cfg.SourceDB)
		if err != nil {
			panic(err)
		}
		dbTables, err = src.GetTablesAccordingToSourceTableRegex(cfg.SourceTable, dbs)
		if err != nil {
			panic(err)
		}
	}

	w := worker.NewWorker(cfg, "", "", "", ig, src)
	wg := sync.WaitGroup{}
	for db, tables := range dbTables {
		for _, table := range tables {
			wg.Add(1)
			w = worker.NewWorker(cfg, db, table, fmt.Sprintf("%s.%s", db, table), ig, src)
			go func() {
				w.Run(ctx)
				wg.Done()
			}()
		}
	}
	wg.Wait()
	targetCount, sourceCount, workerCorrect := w.IsWorkerCorrect()

	if workerCorrect {
		logrus.Infof("Worker %s finished and data correct, source data count is %d,"+
			" target data count is %d", w.Name, sourceCount, targetCount)
	} else {
		logrus.Errorf("Worker %s finished and data incorrect, source data count is %d,"+
			" but databend data count is %d", w.Name, sourceCount, targetCount)
	}

	if w.Cfg.DeleteAfterSync && workerCorrect {
		err := w.Src.DeleteAfterSync()
		if err != nil {
			logrus.Errorf("DeleteAfterSync failed: %v, please do it mannually", err)
		}
	}
	endTime := fmt.Sprintf("end time: %s", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Println(endTime)
	fmt.Println(fmt.Sprintf("total time: %s", time.Since(startTime)))
}

func parseConfigWithFile(configFile string) *config.Config {
	cfg, err := config.LoadConfig(configFile)
	if err != nil {
		panic(err)
	}
	return cfg
}
