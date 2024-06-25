package ingester

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	godatabend "github.com/datafuselabs/databend-go"

	"github.com/databendcloud/db-archiver/config"
	"github.com/databendcloud/db-archiver/source"
)

var (
	ErrUploadStageFailed = errors.New("upload stage failed")
	ErrCopyIntoFailed    = errors.New("copy into failed")
)

type databendIngester struct {
	databendIngesterCfg *config.Config
	statsRecorder       *DatabendIngesterStatsRecorder
}

type DatabendIngester interface {
	IngestData(columns []string, batchJsonData [][]interface{}) error
	uploadToStage(fileName string) (*godatabend.StageLocation, error)
	GetAllSyncedCount() (int, error)
}

func NewDatabendIngester(cfg *config.Config) DatabendIngester {
	stats := NewDatabendIntesterStatsRecorder()
	return &databendIngester{
		databendIngesterCfg: cfg,
		statsRecorder:       stats,
	}
}

func (ig *databendIngester) GetAllSyncedCount() (int, error) {
	db, err := sql.Open("databend", ig.databendIngesterCfg.DatabendDSN)
	defer db.Close()
	if err != nil {
		return 0, err
	}
	rows, err := db.Query(fmt.Sprintf("SELECT count(*) FROM %s WHERE %s",
		ig.databendIngesterCfg.DatabendTable, ig.databendIngesterCfg.SourceWhereCondition))
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	if rows.Next() {
		var count int
		err = rows.Scan(&count)
		if err != nil {
			return 0, err
		}
		return count, nil
	}
	return 0, nil
}

func (ig *databendIngester) IngestData(columns []string, batchData [][]interface{}) error {
	l := logrus.WithFields(logrus.Fields{"ingest_databend": "IngestData"})
	startTime := time.Now()

	if len(batchData) == 0 {
		return nil
	}

	fileName, bytesSize, err := source.GenerateJSONFile(columns, batchData)
	if err != nil {
		l.Errorf("generate NDJson file failed: %v\n", err)
		return err
	}

	stage, err := ig.uploadToStage(fileName)
	if err != nil {
		l.Errorf("upload to stage failed: %v\n", err)
		return err
	}

	err = ig.copyInto(stage)
	if err != nil {
		l.Errorf("copy into failed: %v\n", err)
		return err
	}
	ig.statsRecorder.RecordMetric(bytesSize, len(batchData))
	stats := ig.statsRecorder.Stats(time.Since(startTime))
	log.Printf("ingest %d rows (%f rows/s), %d bytes (%f bytes/s)", len(batchData), stats.RowsPerSecondd, bytesSize, stats.BytesPerSecond)
	return nil
}

func (ig *databendIngester) uploadToStage(fileName string) (*godatabend.StageLocation, error) {
	defer func() {
		err := os.RemoveAll(fileName)
		if err != nil {
			logrus.Errorf("delete batch insert file failed: %v", err)
		}
	}()

	databendConfig, err := godatabend.ParseDSN(ig.databendIngesterCfg.DatabendDSN)
	if err != nil {
		return nil, err
	}
	apiClient := godatabend.NewAPIClientFromConfig(databendConfig)
	fi, err := os.Stat(fileName)
	if err != nil {
		return nil, errors.Wrap(err, "get batch file size failed")
	}
	size := fi.Size()

	f, err := os.Open(fileName)
	if err != nil {
		return nil, errors.Wrap(err, "open batch file failed")
	}
	defer f.Close()
	input := bufio.NewReader(f)
	stage := &godatabend.StageLocation{
		Name: ig.databendIngesterCfg.UserStage,
		Path: fmt.Sprintf("batch/%d-%s", time.Now().Unix(), filepath.Base(fileName)),
	}

	if err := apiClient.UploadToStage(context.Background(), stage, input, size); err != nil {
		return nil, errors.Wrap(ErrUploadStageFailed, err.Error())
	}

	return stage, nil
}

func (ig *databendIngester) copyInto(stage *godatabend.StageLocation) error {
	copyIntoSQL := fmt.Sprintf("COPY INTO %s FROM %s FILE_FORMAT = (type = NDJSON missing_field_as = FIELD_DEFAULT COMPRESSION = AUTO) "+
		"PURGE = %v FORCE = %v DISABLE_VARIANT_CHECK = %v", ig.databendIngesterCfg.DatabendTable, stage.String(),
		ig.databendIngesterCfg.CopyPurge, ig.databendIngesterCfg.CopyForce, ig.databendIngesterCfg.DisableVariantCheck)
	db, err := sql.Open("databend", ig.databendIngesterCfg.DatabendDSN)
	if err != nil {
		logrus.Errorf("create db error: %v", err)
		return err
	}
	if err := execute(db, copyIntoSQL); err != nil {
		return errors.Wrap(ErrCopyIntoFailed, err.Error())
	}
	return nil
}

func execute(db *sql.DB, sql string) error {
	_, err := db.Exec(sql)
	if err != nil {
		logrus.Errorf("exec '%s' failed, err: %v", sql, err)
		return err
	}
	return nil
}
