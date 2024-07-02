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
	if len(data) == 0 {
		return nil
	}
	err = w.ig.IngestData(columns, data)
	if err != nil {
		return err
	}

	return nil
}

func (w *Worker) IsSplitAccordingMaxGoRoutine(minSplitKey, maxSplitKey, batchSize int) bool {
	return (maxSplitKey-minSplitKey)/batchSize > w.cfg.MaxThread
}

func (w *Worker) stepBatch() error {
	wg := &sync.WaitGroup{}
	minSplitKey, maxSplitKey, err := w.src.GetMinMaxSplitKey()
	if err != nil {
		return err
	}
	fmt.Println("minSplitKey", minSplitKey, "maxSplitKey", maxSplitKey)

	if w.IsSplitAccordingMaxGoRoutine(minSplitKey, maxSplitKey, w.cfg.BatchSize) {
		fmt.Println("split according maxGoRoutine", w.cfg.MaxThread)
		slimedRange := w.src.SlimCondition(minSplitKey, maxSplitKey)
		fmt.Println("slimedRange", slimedRange)
		wg.Add(w.cfg.MaxThread)
		for i := 0; i < w.cfg.MaxThread; i++ {
			go func(idx int) {
				defer wg.Done()
				conditions := w.src.SplitConditionAccordingMaxGoRoutine(slimedRange[idx][0], slimedRange[idx][1], maxSplitKey)
				logrus.Infof("conditions in one routine: %v", conditions)
				if err != nil {
					logrus.Errorf("stepBatchWithCondition failed: %v", err)
				}
				for _, condition := range conditions {
					logrus.Infof("condition: %s", condition)
					err := w.stepBatchWithCondition(condition)
					if err != nil {
						logrus.Errorf("stepBatchWithCondition failed: %v", err)
					}
				}
			}(i)
		}
		wg.Wait()
		return nil
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

func (w *Worker) StepBatchByTimeSplitKey() error {
	wg := &sync.WaitGroup{}
	minSplitKey, maxSplitKey, err := w.src.GetMinMaxTimeSplitKey()
	if err != nil {
		return err
	}
	fmt.Println("minSplitKey", minSplitKey, "maxSplitKey", maxSplitKey)

	fmt.Println("split according time split key", w.cfg.MaxThread)
	allConditions, err := w.src.SplitConditionAccordingToTimeSplitKey(minSplitKey, maxSplitKey)
	if err != nil {
		return err
	}
	fmt.Println("all split conditions", allConditions)
	slimedRange := w.src.SplitTimeConditionsByMaxThread(allConditions, w.cfg.MaxThread)
	fmt.Println(len(slimedRange))
	fmt.Println("slimedRange", slimedRange)
	wg.Add(w.cfg.MaxThread)
	for i := 0; i < w.cfg.MaxThread; i++ {
		go func(idx int) {
			defer wg.Done()
			conditions := slimedRange[idx]
			logrus.Infof("conditions in one routine: %v", conditions)
			if err != nil {
				logrus.Errorf("stepBatchWithCondition failed: %v", err)
			}
			for _, condition := range conditions {
				logrus.Infof("condition: %s", condition)
				err := w.stepBatchWithCondition(condition)
				if err != nil {
					logrus.Errorf("stepBatchWithCondition failed: %v", err)
				}
			}
		}(i)
	}
	wg.Wait()
	return nil

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
	if w.cfg.SourceSplitTimeKey != "" {
		err := w.StepBatchByTimeSplitKey()
		if err != nil {
			logrus.Errorf("StepBatchByTimeSplitKey failed: %v", err)
		}
	} else {
		err := w.stepBatch()
		if err != nil {
			logrus.Errorf("stepBatch failed: %v", err)
		}
	}

	if w.cfg.DeleteAfterSync {
		err := w.src.DeleteAfterSync()
		if err != nil {
			logrus.Errorf("DeleteAfterSync failed: %v, please do it mannually", err)
		}
	}
	syncedCount, err := w.ig.GetAllSyncedCount()
	if err != nil {
		logrus.Errorf("GetAllSyncedCount failed: %v", err)
	}

	fmt.Println("all syncedCount is:", syncedCount)
}
