package source

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"

	"github.com/databendcloud/db-archiver/config"
)

type Source struct {
	db  *sql.DB
	cfg *config.Config
}

type Sourcer interface {
	QueryTableData(rows *sql.Rows) ([][]interface{}, []string, error)
	GetBatchNum(rows *sql.Rows) (int, int)
	GetRows(conditionSql string) (*sql.Rows, error)
}

func NewSource(cfg *config.Config) (*Source, error) {
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		cfg.SourceUser,
		cfg.SourcePass,
		cfg.SourceHost,
		cfg.SourcePort,
		cfg.SourceDB))
	if err != nil {
		return nil, err
	}
	return &Source{
		db:  db,
		cfg: cfg,
	}, nil
}

func getRowCount(rows *sql.Rows) int {
	var count = 0
	for rows.Next() {
		count++
	}
	return count
}

func (s *Source) GetSourceReadRowsCount() (int, error) {
	rows, err := s.db.Query(fmt.Sprintf("SELECT * FROM %s.%s WHERE %s", s.cfg.SourceDB,
		s.cfg.SourceTable, s.cfg.SourceWhereCondition))
	if err != nil {
		return 0, err
	}

	defer rows.Close()

	return getRowCount(rows), nil
}

// GetBatchNum returns the number of batches to be processed
func (s *Source) GetBatchNum(rows *sql.Rows) (int, int) {
	count := getRowCount(rows)
	batchNum := count / s.cfg.BatchSize
	if count%s.cfg.BatchSize != 0 {
		batchNum++
	}
	return batchNum, count
}

func (s *Source) GetRows(conditionSql string) (*sql.Rows, error) {
	rows, err := s.db.Query(conditionSql)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (s *Source) GerMinMaxSplitKey() (int, int, error) {
	rows, err := s.db.Query(fmt.Sprintf("select min(%s), max(%s) from %s.%s WHERE %s", s.cfg.SourceSplitKey,
		s.cfg.SourceSplitKey, s.cfg.SourceDB, s.cfg.SourceTable, s.cfg.SourceWhereCondition))
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()

	var minSplitKey, maxSplitKey int
	for rows.Next() {
		err = rows.Scan(&minSplitKey, &maxSplitKey)
		if err != nil {
			return 0, 0, err
		}
	}
	return minSplitKey, maxSplitKey, nil
}

func (s *Source) SlimCondition(minSplitKey, maxSplitKey int) [][]int {
	var conditions [][]int
	if minSplitKey > maxSplitKey {
		return conditions
	}
	rangeSize := (maxSplitKey - minSplitKey) / s.cfg.MaxThread
	for i := 0; i < s.cfg.MaxThread; i++ {
		lowerBound := minSplitKey + rangeSize*i
		upperBound := lowerBound + rangeSize
		if i == s.cfg.MaxThread-1 {
			// Ensure the last condition includes maxSplitKey
			upperBound = maxSplitKey
		}
		conditions = append(conditions, []int{lowerBound, upperBound})
	}
	return conditions
}

func (s *Source) DeleteAfterSync() error {
	if s.cfg.DeleteAfterSync {
		_, err := s.db.Exec(fmt.Sprintf("delete from %s.%s where %s", s.cfg.SourceDB,
			s.cfg.SourceTable, s.cfg.SourceWhereCondition))
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Source) QueryTableData(rows *sql.Rows) ([][]interface{}, []string, error) {
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, nil, err
	}

	scanArgs := make([]interface{}, len(columns))
	for i, columnType := range columnTypes {
		switch columnType.DatabaseTypeName() {
		case "INT", "SMALLINT", "TINYINT", "MEDIUMINT", "BIGINT":
			scanArgs[i] = new(int)
		case "FLOAT", "DOUBLE":
			scanArgs[i] = new(float64)
		case "DECIMAL":
			scanArgs[i] = new(sql.NullFloat64)
		case "CHAR", "VARCHAR", "TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT":
			scanArgs[i] = new(string)
		case "DATE", "TIME", "DATETIME", "TIMESTAMP":
			scanArgs[i] = new(string) // or use time.Time
		case "BOOL", "BOOLEAN":
			scanArgs[i] = new(bool)
		default:
			scanArgs[i] = new(sql.RawBytes)
		}
	}

	var result [][]interface{}
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return nil, nil, err
		}

		row := make([]interface{}, len(columns))
		for i, v := range scanArgs {
			switch v := v.(type) {
			case *int:
				row[i] = *v
			case *string:
				row[i] = *v
			case *bool:
				row[i] = *v
			case *sql.RawBytes:
				row[i] = string(*v)
			}
		}
		result = append(result, row)
	}

	if err = rows.Err(); err != nil {
		return nil, nil, err
	}

	return result, columns, nil
}

func (s *Source) SplitCondition(minSplitKey, maxSplitKey int) []string {
	var conditions []string
	for {
		if minSplitKey >= maxSplitKey {
			conditions = append(conditions, fmt.Sprintf("(%s >= %d and %s <= %d)", s.cfg.SourceSplitKey, minSplitKey, s.cfg.SourceSplitKey, maxSplitKey))
			break
		}
		conditions = append(conditions, fmt.Sprintf("(%s >= %d and %s < %d)", s.cfg.SourceSplitKey, minSplitKey, s.cfg.SourceSplitKey, minSplitKey+s.cfg.BatchSize))
		minSplitKey += s.cfg.BatchSize
	}
	return conditions
}

func (s *Source) SplitConditionAccordingMaxGoRoutine(minSplitKey, maxSplitKey, allMax int) []string {
	var conditions []string
	if minSplitKey > maxSplitKey {
		return conditions
	}

	for {
		if minSplitKey >= maxSplitKey {
			if minSplitKey > allMax {
				return conditions
			}
			if maxSplitKey == allMax {
				conditions = append(conditions, fmt.Sprintf("(%s >= %d and %s <= %d)", s.cfg.SourceSplitKey, minSplitKey, s.cfg.SourceSplitKey, maxSplitKey))
			} else {
				conditions = append(conditions, fmt.Sprintf("(%s >= %d and %s < %d)", s.cfg.SourceSplitKey, minSplitKey, s.cfg.SourceSplitKey, maxSplitKey))
			}
			break
		}
		conditions = append(conditions, fmt.Sprintf("(%s >= %d and %s < %d)", s.cfg.SourceSplitKey, minSplitKey, s.cfg.SourceSplitKey, minSplitKey+s.cfg.BatchSize-1))
		minSplitKey += s.cfg.BatchSize - 1
	}
	return conditions
}

func GenerateJSONFile(columns []string, data [][]interface{}) (string, int, error) {
	l := logrus.WithFields(logrus.Fields{"tardatabend": "IngestData"})
	var batchJsonData []string

	for _, row := range data {
		rowMap := make(map[string]interface{})
		for i, column := range columns {
			rowMap[column] = row[i]
		}
		jsonData, err := json.Marshal(rowMap)
		if err != nil {
			return "", 0, err
		}
		batchJsonData = append(batchJsonData, string(jsonData))
	}

	fileName, bytesSize, err := generateNDJsonFile(batchJsonData)
	if err != nil {
		l.Errorf("generate NDJson file failed: %v\n", err)
		return "", 0, err
	}
	return fileName, bytesSize, nil
}

func generateNDJsonFile(batchJsonData []string) (string, int, error) {
	outputFile, err := ioutil.TempFile("/tmp", "databend-ingest-*.ndjson")
	if err != nil {
		return "", 0, err
	}
	defer outputFile.Close()

	// Create a buffered writer for the Ndjson file
	writer := bufio.NewWriter(outputFile)
	bytesSum := 0

	for _, data := range batchJsonData {
		n, err := writer.WriteString(data + "\n")
		if err != nil {
			return "", 0, err
		}
		bytesSum += n
	}
	// Flush any remaining data to the NDJson file
	err = writer.Flush()
	if err != nil {
		return "", 0, err
	}
	return outputFile.Name(), bytesSum, err
}
