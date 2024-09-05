package source

import (
	"database/sql"
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/databendcloud/db-archiver/config"
)

type MysqlSource struct {
	db            *sql.DB
	cfg           *config.Config
	statsRecorder *DatabendSourceStatsRecorder
}

func NewMysqlSource(cfg *config.Config) (*MysqlSource, error) {
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
	//fmt.Printf("connected to mysql successfully %v", cfg)
	return &MysqlSource{
		db:            db,
		cfg:           cfg,
		statsRecorder: stats,
	}, nil
}

// AdjustBatchSizeAccordingToSourceDbTable has a concept called s,  s = (maxKey - minKey) / sourceTableRowCount
// if s == 1 it means the data is uniform in the table, if s is much bigger than 1, it means the data is not uniform in the table
func (s *MysqlSource) AdjustBatchSizeAccordingToSourceDbTable() int64 {
	minSplitKey, maxSplitKey, err := s.GetMinMaxSplitKey()
	if err != nil {
		return s.cfg.BatchSize
	}
	sourceTableRowCount, err := s.GetSourceReadRowsCount(s.cfg.SourceTable, s.cfg.SourceDB)
	if err != nil {
		return s.cfg.BatchSize
	}
	rangeSize := maxSplitKey - minSplitKey
	switch {
	case int64(sourceTableRowCount) <= s.cfg.BatchSize:
		return rangeSize
	case rangeSize/int64(sourceTableRowCount) >= 10:
		return s.cfg.BatchSize * 5
	case rangeSize/int64(sourceTableRowCount) >= 100:
		return s.cfg.BatchSize * 20
	default:
		return s.cfg.BatchSize
	}
}

func (s *MysqlSource) GetSourceReadRowsCount(table, db string) (int, error) {
	row := s.db.QueryRow(fmt.Sprintf("SELECT count(*) FROM %s.%s WHERE %s", db,
		table, s.cfg.SourceWhereCondition))
	var rowCount int
	err := row.Scan(&rowCount)
	if err != nil {
		return 0, err
	}

	return rowCount, nil
}

func (s *MysqlSource) GetMinMaxSplitKey() (int64, int64, error) {
	rows, err := s.db.Query(fmt.Sprintf("select min(%s), max(%s) from %s.%s WHERE %s", s.cfg.SourceSplitKey,
		s.cfg.SourceSplitKey, s.cfg.SourceDB, s.cfg.SourceTable, s.cfg.SourceWhereCondition))
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()

	var minSplitKey, maxSplitKey sql.NullInt64
	for rows.Next() {
		err = rows.Scan(&minSplitKey, &maxSplitKey)
		if err != nil {
			return 0, 0, err
		}
	}

	// Check if minSplitKey and maxSplitKey are valid (not NULL)
	if !minSplitKey.Valid || !maxSplitKey.Valid {
		return 0, 0, nil
	}

	return minSplitKey.Int64, maxSplitKey.Int64, nil
}

func (s *MysqlSource) GetMinMaxTimeSplitKey() (string, string, error) {
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

func (s *MysqlSource) DeleteAfterSync() error {
	if s.cfg.DeleteAfterSync {
		_, err := s.db.Exec(fmt.Sprintf("delete from %s.%s where %s", s.cfg.SourceDB,
			s.cfg.SourceTable, s.cfg.SourceWhereCondition))
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *MysqlSource) QueryTableData(threadNum int, conditionSql string) ([][]interface{}, []string, error) {
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

func (s *MysqlSource) GetDatabasesAccordingToSourceDbRegex(sourceDatabasePattern string) ([]string, error) {
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

func (s *MysqlSource) GetTablesAccordingToSourceTableRegex(sourceTablePattern string, databases []string) (map[string][]string, error) {
	dbTables := make(map[string][]string)
	for _, database := range databases {
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

func (s *MysqlSource) GetAllSourceReadRowsCount() (int, error) {
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

func (s *MysqlSource) GetDbTablesAccordingToSourceDbTables() (map[string][]string, error) {
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
