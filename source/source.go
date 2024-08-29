package source

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"

	"github.com/databendcloud/db-archiver/config"
)

type Source struct {
	db            *sql.DB
	cfg           *config.Config
	statsRecorder *DatabendSourceStatsRecorder
}

type Sourcer interface {
	QueryTableData(threadNum int, conditionSql string) ([][]interface{}, []string, error)
}

func NewSource(cfg *config.Config) (*Source, error) {
	stats := NewDatabendIntesterStatsRecorder()
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/mysql",
		cfg.SourceUser,
		cfg.SourcePass,
		cfg.SourceHost,
		cfg.SourcePort))
	if err != nil {
		logrus.Errorf("failed to open db: %v", err)
		return nil, err
	}
	fmt.Printf("connected to mysql successfully %v", cfg)
	return &Source{
		db:            db,
		cfg:           cfg,
		statsRecorder: stats,
	}, nil
}

func (s *Source) GetAllSourceReadRowsCount() (int, error) {
	allCount := 0

	dbTables, err := s.GetDbTablesAccordingToSourceDbTables()
	if err != nil {
		return 0, err
	}
	for db, tables := range dbTables {
		for _, table := range tables {
			count, err := s.GetSourceReadRowsCount(table, db)
			if err != nil {
				return 0, err
			}
			allCount += count
		}
	}

	return allCount, nil
}

func (s *Source) GetSourceReadRowsCount(table, db string) (int, error) {
	row := s.db.QueryRow(fmt.Sprintf("SELECT count(*) FROM %s.%s WHERE %s", db,
		table, s.cfg.SourceWhereCondition))
	var rowCount int
	err := row.Scan(&rowCount)
	if err != nil {
		return 0, err
	}

	return rowCount, nil
}

func (s *Source) GetRowsCountByConditionSql(conditionSql string) (int, error) {
	rowCountQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s WHERE %s", s.cfg.SourceDB, s.cfg.SourceTable, conditionSql)
	row := s.db.QueryRow(rowCountQuery)
	var rowCount int
	err := row.Scan(&rowCount)
	if err != nil {
		return 0, err
	}

	return rowCount, nil
}

func (s *Source) GetMinMaxSplitKey() (int, int, error) {
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

func (s *Source) GetMinMaxTimeSplitKey() (string, string, error) {
	rows, err := s.db.Query(fmt.Sprintf("select min(%s), max(%s) from %s.%s WHERE %s", s.cfg.SourceSplitTimeKey,
		s.cfg.SourceSplitTimeKey, s.cfg.SourceDB, s.cfg.SourceTable, s.cfg.SourceWhereCondition))
	if err != nil {
		return "", "", err
	}
	defer rows.Close()

	var minSplitKey, maxSplitKey string
	for rows.Next() {
		err = rows.Scan(&minSplitKey, &maxSplitKey)
		if err != nil {
			return "", "", err
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

func (s *Source) QueryTableData(threadNum int, conditionSql string) ([][]interface{}, []string, error) {
	startTime := time.Now()
	execSql := fmt.Sprintf("SELECT * FROM %s.%s WHERE %s", s.cfg.SourceDB,
		s.cfg.SourceTable, conditionSql)
	if s.cfg.SourceWhereCondition != "" && s.cfg.SourceSplitKey != "" {
		execSql = fmt.Sprintf("%s AND %s", execSql, s.cfg.SourceWhereCondition)
	}
	rows, err := s.db.Query(execSql)
	if err != nil {
		return nil, nil, err
	}
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
			scanArgs[i] = new(sql.NullInt64)
		case "UNSIGNED INT", "UNSIGNED TINYINT", "UNSIGNED MEDIUMINT", "UNSIGNED BIGINT":
			scanArgs[i] = new(sql.NullInt64)
		case "FLOAT", "DOUBLE":
			scanArgs[i] = new(sql.NullFloat64)
		case "DECIMAL":
			scanArgs[i] = new(sql.NullFloat64)
		case "CHAR", "VARCHAR", "TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT":
			scanArgs[i] = new(sql.NullString)
		case "DATE", "TIME", "DATETIME", "TIMESTAMP":
			scanArgs[i] = new(sql.NullString) // or use time.Time
		case "BOOL", "BOOLEAN":
			scanArgs[i] = new(sql.NullBool)
		default:
			scanArgs[i] = new(sql.RawBytes)
		}
	}

	var result [][]interface{}
	//rowCount, err := s.GetRowsCountByConditionSql(conditionSql)
	//if err != nil {
	//	return nil, nil, err
	//}
	//result := make([][]interface{}, rowCount)
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
			case *sql.NullString:
				if v.Valid {
					row[i] = v.String
				} else {
					row[i] = nil
				}
			case *bool:
				row[i] = *v
			case *sql.NullInt64:
				if v.Valid {
					row[i] = v.Int64
				} else {
					row[i] = nil
				}
			case *sql.NullFloat64:
				if v.Valid {
					row[i] = v.Float64
				} else {
					row[i] = nil
				}
			case *sql.NullBool:
				if v.Valid {
					row[i] = v.Bool
				} else {
					row[i] = nil
				}
			case *float64:
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
	s.statsRecorder.RecordMetric(len(result))
	stats := s.statsRecorder.Stats(time.Since(startTime))
	log.Printf("thread-%d: extract %d rows (%f rows/s)", threadNum, len(result), stats.RowsPerSecondd)

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
		if (minSplitKey + s.cfg.BatchSize - 1) >= maxSplitKey {
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
		if (minSplitKey + s.cfg.BatchSize - 1) >= allMax {
			conditions = append(conditions, fmt.Sprintf("(%s >= %d and %s <= %d)", s.cfg.SourceSplitKey, minSplitKey, s.cfg.SourceSplitKey, allMax))
			return conditions
		}
		conditions = append(conditions, fmt.Sprintf("(%s >= %d and %s < %d)", s.cfg.SourceSplitKey, minSplitKey, s.cfg.SourceSplitKey, minSplitKey+s.cfg.BatchSize-1))
		minSplitKey += s.cfg.BatchSize - 1
	}
	return conditions
}

func (s *Source) SplitTimeConditionsByMaxThread(conditions []string, maxThread int) [][]string {
	// If maxThread is greater than the length of conditions, return conditions as a single group
	if maxThread >= len(conditions) {
		return [][]string{conditions}
	}
	var splitConditions [][]string
	chunkSize := (len(conditions) + maxThread - 1) / maxThread
	for i := 0; i < len(conditions); i += chunkSize {
		end := i + chunkSize
		if end > len(conditions) {
			end = len(conditions)
		}
		splitConditions = append(splitConditions, conditions[i:end])
	}
	return splitConditions
}

func (s *Source) SplitConditionAccordingToTimeSplitKey(minTimeSplitKey, maxTimeSplitKey string) ([]string, error) {
	var conditions []string

	// Parse the time strings
	minTime, err := time.Parse("2006-01-02 15:04:05", minTimeSplitKey)
	if err != nil {
		return nil, err
	}

	maxTime, err := time.Parse("2006-01-02 15:04:05", maxTimeSplitKey)
	if err != nil {
		return nil, err
	}
	if minTime.After(maxTime) {
		return conditions, nil
	}

	// Iterate over the time range
	for {
		if minTime.After(maxTime) {
			conditions = append(conditions, fmt.Sprintf("(%s >= '%s' and %s <= '%s')", s.cfg.SourceSplitTimeKey, minTime.Format("2006-01-02 15:04:05"), s.cfg.SourceSplitTimeKey, maxTime.Format("2006-01-02 15:04:05")))
			break
		}
		if minTime.Equal(maxTime) {
			conditions = append(conditions, fmt.Sprintf("(%s >= '%s' and %s <= '%s')", s.cfg.SourceSplitTimeKey, minTime.Format("2006-01-02 15:04:05"), s.cfg.SourceSplitTimeKey, maxTime.Format("2006-01-02 15:04:05")))
			break
		}
		conditions = append(conditions, fmt.Sprintf("(%s >= '%s' and %s < '%s')", s.cfg.SourceSplitTimeKey, minTime.Format("2006-01-02 15:04:05"), s.cfg.SourceSplitTimeKey, minTime.Add(s.cfg.GetTimeRangeBySplitUnit()).Format("2006-01-02 15:04:05")))
		minTime = minTime.Add(s.cfg.GetTimeRangeBySplitUnit())
	}

	return conditions, nil
}

func (s *Source) GetDatabasesAccordingToSourceDbRegex(sourceDatabasePattern string) ([]string, error) {
	rows, err := s.db.Query("SHOW DATABASES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var database string
		err = rows.Scan(&database)
		if err != nil {
			return nil, err
		}
		fmt.Println("sourcedatabase pattern", sourceDatabasePattern)
		match, err := regexp.MatchString(sourceDatabasePattern, database)
		if err != nil {
			return nil, err
		}
		if match {
			fmt.Println("match db: ", database)
			databases = append(databases, database)
		}
	}
	return databases, nil
}

func (s *Source) GetTablesAccordingToSourceTableRegex(sourceTablePattern string, databases []string) (map[string][]string, error) {
	dbTables := make(map[string][]string)
	for _, database := range databases {
		fmt.Println("database: ", database)
		rows, err := s.db.Query(fmt.Sprintf("SHOW TABLES FROM %s", database))
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		var tables []string
		for rows.Next() {
			var table string
			err = rows.Scan(&table)
			if err != nil {
				return nil, err
			}
			match, err := regexp.MatchString(sourceTablePattern, table)
			if err != nil {
				return nil, err
			}
			if match {
				tables = append(tables, table)
			}
		}
		dbTables[database] = tables
	}
	return dbTables, nil
}

func (s *Source) GetDbTablesAccordingToSourceDbTables() (map[string][]string, error) {
	allDbTables := make(map[string][]string)
	for _, sourceDbTable := range s.cfg.SourceDbTables {
		dbTable := strings.Split(sourceDbTable, "@") // because `.` in regex is a special character, so use `@` to split
		if len(dbTable) != 2 {
			return nil, fmt.Errorf("invalid sourceDbTable: %s, should be a.b format", sourceDbTable)
		}
		dbs, err := s.GetDatabasesAccordingToSourceDbRegex(dbTable[0])
		if err != nil {
			return nil, fmt.Errorf("get databases according to sourceDbRegex failed: %v", err)
		}
		fmt.Println("dbTable-sjh", dbTable)
		dbTables, err := s.GetTablesAccordingToSourceTableRegex(dbTable[1], dbs)
		if err != nil {
			return nil, fmt.Errorf("get tables according to sourceTableRegex failed: %v", err)
		}
		for db, tables := range dbTables {
			allDbTables[db] = append(allDbTables[db], tables...)
		}
	}
	return allDbTables, nil
}

func GenerateJSONFile(columns []string, data [][]interface{}) (string, int, error) {
	l := logrus.WithFields(logrus.Fields{"tardatabend": "IngestData"})
	var batchJsonData []string

	for _, row := range data {
		if len(row) == 0 {
			continue
		}
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
	fileName := fmt.Sprintf("databend-ingest-%d.ndjson", time.Now().UnixNano())
	outputFile, err := os.CreateTemp("/tmp", fileName)
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
