package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/databendcloud/db-archiver/config"
	"github.com/databendcloud/db-archiver/ingester"
	"github.com/databendcloud/db-archiver/source"
)

type Worker struct {
	name          string
	cfg           *config.Config
	ig            ingester.DatabendIngester
	src           *source.Source
	statsRecorder *DatabendWorkerStatsRecorder
}

var (
	AlreadyIngestRows  = 0
	AlreadyIngestBytes = 0
)

func NewWorker(cfg *config.Config, dbName string, tableName string, name string, ig ingester.DatabendIngester, src *source.Source) *Worker {
	stats := NewDatabendWorkerStatsRecorder()
	cfg.SourceDB = dbName
	cfg.SourceTable = tableName
	cfg.SourceQuery = fmt.Sprintf("select * from %s.%s", cfg.SourceDB, cfg.SourceTable)
	src, err := source.NewSource(cfg)
	if err != nil {
		return nil
	}

	return &Worker{
		name:          name,
		cfg:           cfg,
		ig:            ig,
		src:           src,
		statsRecorder: stats,
	}
}

func (w *Worker) stepBatchWithCondition(threadNum int, conditionSql string) error {
	data, columns, err := w.src.QueryTableData(threadNum, conditionSql)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	startTime := time.Now()
	err = w.ig.DoRetry(
		func() error {
			return w.ig.IngestData(threadNum, columns, data)
		})
	AlreadyIngestRows += len(data)
	AlreadyIngestBytes += calculateBytesSize(data)
	w.statsRecorder.RecordMetric(AlreadyIngestBytes, AlreadyIngestRows)
	stats := w.statsRecorder.Stats(time.Since(startTime))
	log.Printf("Globla speed: total ingested %d rows (%f rows/s), %d bytes (%f bytes/s)",
		AlreadyIngestRows, stats.RowsPerSecondd, AlreadyIngestBytes, stats.BytesPerSecond)

	if err != nil {
		logrus.Errorf("Failed to ingest data between %s into Databend: %v", conditionSql, err)
		return err
	}

	return nil
}

func calculateBytesSize(batch [][]interface{}) int {
	bytes, err := json.Marshal(batch)
	if err != nil {
		log.Fatal(err)
	}
	return len(bytes)
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
				logrus.Infof("conditions in one routine: %v", len(conditions))
				if err != nil {
					logrus.Errorf("stepBatchWithCondition failed: %v", err)
				}
				for _, condition := range conditions {
					logrus.Infof("condition: %s", condition)
					err := w.stepBatchWithCondition(idx, condition)
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
			err := w.stepBatchWithCondition(1, condition)
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
	fmt.Println("allConditions: ", len(allConditions))
	fmt.Println("all split conditions", allConditions)
	slimedRange := w.src.SplitTimeConditionsByMaxThread(allConditions, w.cfg.MaxThread)
	fmt.Println(len(slimedRange))
	fmt.Println("slimedRange", slimedRange)
	wg.Add(w.cfg.MaxThread)
	for i := 0; i < w.cfg.MaxThread; i++ {
		go func(idx int) {
			defer wg.Done()
			conditions := slimedRange[idx]
			logrus.Infof("conditions in one routine: %d", len(conditions))
			if err != nil {
				logrus.Errorf("stepBatchWithCondition failed: %v", err)
			}
			for _, condition := range conditions {
				logrus.Infof("condition: %s", condition)
				err := w.stepBatchWithTimeCondition(condition, w.cfg.BatchSize)
				if err != nil {
					logrus.Errorf("stepBatchWithCondition failed: %v", err)
				}
			}
		}(i)
	}
	wg.Wait()

	return nil
}

func (w *Worker) stepBatchWithTimeCondition(conditionSql string, batchSize int) error {
	offset := 0
	for {
		batchSql := fmt.Sprintf("%s LIMIT %d OFFSET %d", conditionSql, batchSize, offset)
		data, columns, err := w.src.QueryTableData(1, batchSql)
		if err != nil {
			return err
		}
		if len(data) == 0 {
			break
		}
		err = w.ig.DoRetry(
			func() error {
				return w.ig.IngestData(1, columns, data)
			})
		if err != nil {
			logrus.Errorf("Failed to ingest data between %s into Databend: %v", conditionSql, err)
			return err
		}
		offset += batchSize
	}
	return nil
}

func (w *Worker) IsWorkerCorrect() (int, int, bool) {
	syncedCount, err := w.ig.GetAllSyncedCount()
	if err != nil {
		return 0, 0, false
	}
	sourceCount, err := w.src.GetSourceReadRowsCount()
	if err != nil {
		return 0, 0, false
	}
	return syncedCount, sourceCount, syncedCount == sourceCount
}

func (w *Worker) Run(ctx context.Context) {
	logrus.Printf("Worker %s checking before start", w.name)
	syncedCount, err := w.ig.GetAllSyncedCount()
	if err != nil || syncedCount != 0 {
		logrus.Errorf("pre-check failed: %v", err)
		return
	}

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

	sourceCount, targetCount, workerCorrect := w.IsWorkerCorrect()

	if workerCorrect {
		logrus.Infof("Worker %s finished and data correct, source data count is %d,"+
			" target data count is %d", w.name, sourceCount, targetCount)
	} else {
		logrus.Errorf("Worker %s finished and data incorrect, source data count is %d,"+
			" but databend data count is %d", w.name, sourceCount, targetCount)
	}

	if w.cfg.DeleteAfterSync && workerCorrect {
		err := w.src.DeleteAfterSync()
		if err != nil {
			logrus.Errorf("DeleteAfterSync failed: %v, please do it mannually", err)
		}
	}
}
