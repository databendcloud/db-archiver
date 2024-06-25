package worker

import (
	"context"
	"fmt"
	"sync"

	"github.com/sirupsen/logrus"

	"github.com/databendcloud/db-archiver/config"
	"github.com/databendcloud/db-archiver/ingester"
	"github.com/databendcloud/db-archiver/source"
)

type Worker struct {
	name string
	cfg  *config.Config
	ig   ingester.DatabendIngester
	src  *source.Source
}

func NewWorker(cfg *config.Config, name string, ig ingester.DatabendIngester, src *source.Source) *Worker {
	return &Worker{
		name: name,
		cfg:  cfg,
		ig:   ig,
		src:  src,
	}
}

func (w *Worker) stepBatchWithCondition(conditionSql string) error {
	execSql := fmt.Sprintf("SELECT * FROM %s.%s WHERE %s", w.cfg.SourceDB,
		w.cfg.SourceTable, conditionSql)
	if w.cfg.SourceWhereCondition != "" {
		execSql = fmt.Sprintf("%s AND %s", execSql, w.cfg.SourceWhereCondition)
	}
	rows, err := w.src.GetRows(execSql)
	if err != nil {
		return err
	}

	data, columns, err := w.src.QueryTableData(rows)
	if err != nil {
		return err
	}
	err = w.ig.IngestData(columns, data)
	if err != nil {
		return err
	}

	return nil
}

func (w *Worker) stepBatch() error {
	wg := &sync.WaitGroup{}
	minSplitKey, maxSplitKey, err := w.src.GerMinMaxSplitKey()
	if err != nil {
		return err
	}
	conditions := w.src.SplitCondition(minSplitKey, maxSplitKey)
	for _, condition := range conditions {
		wg.Add(1)
		go func(condition string) {
			defer wg.Done()
			err := w.stepBatchWithCondition(condition)
			if err != nil {
				logrus.Errorf("stepBatchWithCondition failed: %v", err)
			}
		}(condition)
	}
	wg.Wait()
	return nil
}

func (w *Worker) IsWorkerCorrect() bool {
	syncedCount, err := w.ig.GetAllSyncedCount()
	if err != nil {
		return false
	}
	sourceCount, err := w.src.GetSourceReadRowsCount()
	if err != nil {
		return false
	}
	return syncedCount == sourceCount
}

func (w *Worker) Run(ctx context.Context) {
	logrus.Printf("Starting worker %s", w.name)
	w.stepBatch()

	if w.cfg.DeleteAfterSync {
		err := w.src.DeleteAfterSync()
		if err != nil {
			logrus.Errorf("DeleteAfterSync failed: %v, please do it mannually", err)
		}
	}
}
