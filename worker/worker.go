package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/databendcloud/db-archiver/config"
	"github.com/databendcloud/db-archiver/ingester"
	"github.com/databendcloud/db-archiver/source"
)

type Worker struct {
	Name          string
	Cfg           *config.Config
	Ig            ingester.DatabendIngester
	Src           source.Sourcer
	statsRecorder *DatabendWorkerStatsRecorder
}

var (
	AlreadyIngestRows  = 0
	AlreadyIngestBytes = 0
)

func NewWorker(cfg *config.Config, name string, ig ingester.DatabendIngester, src source.Sourcer) *Worker {
	stats := NewDatabendWorkerStatsRecorder()
	cfg.SourceQuery = fmt.Sprintf("select * from %s.%s", cfg.SourceDB, cfg.SourceTable)

	return &Worker{
		Name:          name,
		Cfg:           cfg,
		Ig:            ig,
		Src:           src,
		statsRecorder: stats,
	}
}

func (w *Worker) stepBatchWithCondition(threadNum int, conditionSql string) error {
	data, columns, err := w.Src.QueryTableData(threadNum, conditionSql)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	startTime := time.Now()
	err = w.Ig.DoRetry(
		func() error {
			return w.Ig.IngestData(threadNum, columns, data)
		})
	AlreadyIngestRows += len(data)
	AlreadyIngestBytes += calculateBytesSize(data)
	w.statsRecorder.RecordMetric(AlreadyIngestBytes, AlreadyIngestRows)
	stats := w.statsRecorder.Stats(time.Since(startTime))
	log.Printf("Globla speed: total ingested %d rows (%f rows/s), %d bytes (%f bytes/s)",
		AlreadyIngestRows, stats.RowsPerSecond, AlreadyIngestBytes, stats.BytesPerSecond)

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

// IsSplitAccordingMaxGoRoutine checks if the split key is according to the max go routine
func (w *Worker) IsSplitAccordingMaxGoRoutine(minSplitKey, maxSplitKey, batchSize int64) bool {
	return (maxSplitKey-minSplitKey)/batchSize > int64(w.Cfg.MaxThread)
}

func (w *Worker) stepBatch() error {
	wg := &sync.WaitGroup{}
	minSplitKey, maxSplitKey, err := w.Src.GetMinMaxSplitKey()
	if err != nil {
		return err
	}
	if minSplitKey == 0 && maxSplitKey == 0 {
		logrus.Infof("db.table is %s.%s, minSplitKey: %d, maxSplitKey : %d", w.Cfg.SourceDB, w.Cfg.SourceTable, minSplitKey, maxSplitKey)
		return nil
	}
	logrus.Infof("db.table is %s.%s, minSplitKey: %d, maxSplitKey : %d", w.Cfg.SourceDB, w.Cfg.SourceTable, minSplitKey, maxSplitKey)

	if w.IsSplitAccordingMaxGoRoutine(minSplitKey, maxSplitKey, w.Cfg.BatchSize) {
		fmt.Println("split according maxGoRoutine", w.Cfg.MaxThread)
		slimedRange := source.SlimCondition(w.Cfg.MaxThread, minSplitKey, maxSplitKey)
		fmt.Println("slimedRange", slimedRange)
		wg.Add(w.Cfg.MaxThread)
		for i := 0; i < w.Cfg.MaxThread; i++ {
			go func(idx int) {
				defer wg.Done()
				conditions := source.SplitConditionAccordingMaxGoRoutine(w.Cfg.SourceSplitKey, w.Cfg.BatchSize, slimedRange[idx][0], slimedRange[idx][1], maxSplitKey)
				logrus.Infof("conditions in one routine: %v", len(conditions))
				if err != nil {
					logrus.Errorf("stepBatchWithCondition failed: %v", err)
				}
				for condition := range conditions {
					logrus.Infof("condition: %s", condition)
					err := w.stepBatchWithCondition(idx, condition)
					if err != nil {
						logrus.Errorf("Thread %d, stepBatchWithCondition failed: %v", idx, err)
					}
				}
			}(i)
		}
		wg.Wait()
		return nil
	}
	conditions := source.SplitCondition(w.Cfg.SourceSplitKey, w.Cfg.BatchSize, minSplitKey, maxSplitKey)
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
	minSplitKey, maxSplitKey, err := w.Src.GetMinMaxTimeSplitKey()
	if err != nil {
		return err
	}
	fmt.Println("minSplitKey", minSplitKey, "maxSplitKey", maxSplitKey)

	fmt.Println("split according time split key", w.Cfg.MaxThread)
	allConditions, err := source.SplitConditionAccordingToTimeSplitKey(w.Cfg, minSplitKey, maxSplitKey)
	if err != nil {
		return err
	}
	fmt.Println("allConditions: ", len(allConditions))
	fmt.Println("all split conditions", allConditions)
	slimedRange := source.SplitTimeConditionsByMaxThread(allConditions, w.Cfg.MaxThread)
	fmt.Println(len(slimedRange))
	fmt.Println("slimedRange", slimedRange)
	wg.Add(w.Cfg.MaxThread)
	for i := 0; i < w.Cfg.MaxThread; i++ {
		go func(idx int) {
			defer wg.Done()
			conditions := slimedRange[idx]
			logrus.Infof("conditions in one routine: %d", len(conditions))
			if err != nil {
				logrus.Errorf("stepBatchWithCondition failed: %v", err)
			}
			for _, condition := range conditions {
				logrus.Infof("condition: %s", condition)
				switch w.Cfg.DatabaseType {
				case "mysql":
					err = w.stepBatchWithTimeCondition(condition, w.Cfg.BatchSize)
				case "mssql":
					err = w.stepBatchWithTimeConditionMssql(condition, w.Cfg.BatchSize)
				default:
					err = w.stepBatchWithTimeCondition(condition, w.Cfg.BatchSize)
				}
				if err != nil {
					logrus.Errorf("stepBatchWithCondition failed: %v", err)
				}
			}
		}(i)
	}
	wg.Wait()

	return nil
}

func (w *Worker) stepBatchWithTimeCondition(conditionSql string, batchSize int64) error {
	var offset int64 = 0
	for {
		batchSql := fmt.Sprintf("%s LIMIT %d OFFSET %d", conditionSql, batchSize, offset)
		data, columns, err := w.Src.QueryTableData(1, batchSql)
		if err != nil {
			return err
		}
		if len(data) == 0 {
			break
		}
		err = w.Ig.DoRetry(
			func() error {
				return w.Ig.IngestData(1, columns, data)
			})
		if err != nil {
			logrus.Errorf("Failed to ingest data between %s into Databend: %v", conditionSql, err)
			return err
		}
		offset += batchSize
	}
	return nil
}

func (w *Worker) stepBatchWithTimeConditionMssql(conditionSql string, batchSize int64) error {
	var offset int64 = 0
	conditionSql = ensureOrderBy(conditionSql)
	fmt.Println("conditionSql", conditionSql)
	for {
		batchSql := fmt.Sprintf("%s OFFSET %d ROWS FETCH NEXT %d ROWS ONLY", conditionSql, offset, batchSize)

		data, columns, err := w.Src.QueryTableData(1, batchSql)
		if err != nil {
			return err
		}

		if len(data) == 0 {
			break
		}

		err = w.Ig.DoRetry(
			func() error {
				return w.Ig.IngestData(1, columns, data)
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
	syncedCount, err := w.Ig.GetAllSyncedCount()
	if err != nil {
		logrus.Errorf("GetAllSyncedCount failed: %v", err)
		return 0, 0, false
	}
	sourceCount, err := w.Src.GetAllSourceReadRowsCount()
	if err != nil {
		logrus.Errorf("GetAllSourceReadRowsCount failed: %v", err)
		return 0, 0, false
	}
	return syncedCount, sourceCount, syncedCount == sourceCount
}

func (w *Worker) Run(ctx context.Context) {
	logrus.Printf("Worker %s checking before start", w.Name)

	logrus.Printf("Starting worker %s", w.Name)
	if w.Cfg.SourceSplitTimeKey != "" {
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
}

func ensureOrderBy(conditionSql string) string {
	if !strings.Contains(strings.ToLower(conditionSql), "order by") {
		conditionSql += " ORDER BY id"
	}
	return conditionSql
}
