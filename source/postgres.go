package source

import (
	"database/sql"
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"

	"github.com/databendcloud/db-archiver/config"
)

type PostgresSource struct {
	db            *sql.DB
	cfg           *config.Config
	statsRecorder *DatabendSourceStatsRecorder
}

func (p PostgresSource) AdjustBatchSizeAccordingToSourceDbTable() int64 {
	minSplitKey, maxSplitKey, err := p.GetMinMaxSplitKey()
	if err != nil {
		return p.cfg.BatchSize
	}
	sourceTableRowCount, err := p.GetSourceReadRowsCount()
	if err != nil {
		return p.cfg.BatchSize
	}
	rangeSize := maxSplitKey - minSplitKey + 1
	switch {
	case int64(sourceTableRowCount) <= p.cfg.BatchSize:
		return rangeSize
	case rangeSize/int64(sourceTableRowCount) >= 10:
		return p.cfg.BatchSize * 5
	case rangeSize/int64(sourceTableRowCount) >= 100:
		return p.cfg.BatchSize * 20
	default:
		return p.cfg.BatchSize
	}
}

func NewPostgresSource(cfg *config.Config) (*PostgresSource, error) {
	stats := NewDatabendIntesterStatsRecorder()
	// disable - No SSL
	//require - Always SSL (skip verification)
	//verify-ca - Always SSL (verify that the certificate presented by the server was signed by a trusted CA)
	//verify-full - Always SSL (verify that the certification presented by the server was signed by a trusted CA and the server host name matches the one in the certificate)
	if cfg.SSLMode == "" {
		cfg.SSLMode = "disable"
	}
	db, err := sql.Open("postgres", fmt.Sprintf("postgres://%s:%s@%s:%d/postgres?sslmode=%s",
		cfg.SourceUser,
		cfg.SourcePass,
		cfg.SourceHost,
		cfg.SourcePort,
		cfg.SSLMode))
	if err != nil {
		logrus.Errorf("failed to open postgres db: %v", err)
		return nil, err
	}
	return &PostgresSource{
		db:            db,
		cfg:           cfg,
		statsRecorder: stats,
	}, nil
}

func (p *PostgresSource) SwitchDatabase() error {
	// Close the current connection
	err := p.db.Close()
	if err != nil {
		return err
	}

	// Open a new connection to the new database
	db, err := sql.Open("postgres", fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		p.cfg.SourceUser,
		p.cfg.SourcePass,
		p.cfg.SourceHost,
		p.cfg.SourcePort,
		p.cfg.SourceDB,
		p.cfg.SSLMode))
	if err != nil {
		return err
	}

	// Replace the old connection with the new one
	p.db = db
	return nil
}
func (p PostgresSource) GetSourceReadRowsCount() (int, error) {
	err := p.SwitchDatabase()
	if err != nil {
		return 0, err
	}
	row := p.db.QueryRow(fmt.Sprintf("SELECT count(*) FROM %s WHERE %s",
		p.cfg.SourceTable, p.cfg.SourceWhereCondition))
	var rowCount int
	err = row.Scan(&rowCount)
	if err != nil {
		return 0, err
	}

	return rowCount, nil
}

func (p PostgresSource) GetMinMaxSplitKey() (int64, int64, error) {
	err := p.SwitchDatabase()
	if err != nil {
		return 0, 0, err
	}
	rows, err := p.db.Query(fmt.Sprintf("select COALESCE(min(%s),0), COALESCE(max(%s),0) from %s WHERE %s",
		p.cfg.SourceSplitKey, p.cfg.SourceSplitKey, p.cfg.SourceTable, p.cfg.SourceWhereCondition))
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

func (p PostgresSource) GetMinMaxTimeSplitKey() (string, string, error) {
	err := p.SwitchDatabase()
	if err != nil {
		return "", "", err
	}
	rows, err := p.db.Query(fmt.Sprintf("select min(%s), max(%s) from %s WHERE %s", p.cfg.SourceSplitTimeKey,
		p.cfg.SourceSplitTimeKey, p.cfg.SourceTable, p.cfg.SourceWhereCondition))
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

func (p PostgresSource) DeleteAfterSync() error {
	err := p.SwitchDatabase()
	if err != nil {
		return err
	}
	if p.cfg.DeleteAfterSync {
		_, err := p.db.Exec(fmt.Sprintf("delete from %s where %s",
			p.cfg.SourceTable, p.cfg.SourceWhereCondition))
		if err != nil {
			return err
		}
	}
	return nil
}

func (p PostgresSource) QueryTableData(threadNum int, conditionSql string) ([][]interface{}, []string, error) {
	startTime := time.Now()
	err := p.SwitchDatabase()
	if err != nil {
		return nil, nil, err
	}
	execSql := fmt.Sprintf("SELECT * FROM %s WHERE %s",
		p.cfg.SourceTable, conditionSql)
	if p.cfg.SourceWhereCondition != "" && p.cfg.SourceSplitKey != "" {
		execSql = fmt.Sprintf("%s AND %s", execSql, p.cfg.SourceWhereCondition)
	}
	rows, err := p.db.Query(execSql)
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
		fmt.Printf("%s\n", columnType.DatabaseTypeName())
		switch columnType.DatabaseTypeName() {
		case "INT", "SMALLINT", "TINYINT", "MEDIUMINT", "BIGINT", "INT4", "INT8":
			scanArgs[i] = new(sql.NullInt64)
		case "UNSIGNED INT", "UNSIGNED TINYINT", "UNSIGNED MEDIUMINT", "UNSIGNED BIGINT":
			scanArgs[i] = new(sql.NullInt64)
		case "FLOAT", "DOUBLE", "FLOAT8":
			scanArgs[i] = new(sql.NullFloat64)
		case "DECIMAL", "NUMERIC":
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
					//row[i] = v.Bool
					if v.Bool {
						row[i] = 1
					} else {
						row[i] = 0
					}
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
	p.statsRecorder.RecordMetric(len(result))
	stats := p.statsRecorder.Stats(time.Since(startTime))
	log.Printf("thread-%d: extract %d rows (%f rows/s)", threadNum, len(result), stats.RowsPerSecondd)

	return result, columns, nil
}

func (p PostgresSource) GetDatabasesAccordingToSourceDbRegex(sourceDatabasePattern string) ([]string, error) {
	rows, err := p.db.Query("SELECT datname FROM pg_database")
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
		match, err := regexp.MatchString(sourceDatabasePattern, database)
		if err != nil {
			return nil, err
		}
		if match {
			databases = append(databases, database)
		}
	}
	return databases, nil
}

func (p PostgresSource) GetTablesAccordingToSourceTableRegex(sourceTablePattern string, databases []string) (map[string][]string, error) {
	dbTables := make(map[string][]string)
	for _, database := range databases {
		p.cfg.SourceDB = database
		err := p.SwitchDatabase()
		if err != nil {
			return nil, err
		}
		rows, err := p.db.Query(fmt.Sprintf("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'"))
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

func (p PostgresSource) GetAllSourceReadRowsCount() (int, error) {
	allCount := 0

	dbTables, err := p.GetDbTablesAccordingToSourceDbTables()
	if err != nil {
		return 0, err
	}
	for db, tables := range dbTables {
		p.cfg.SourceDB = db
		for _, table := range tables {
			p.cfg.SourceTable = table
			count, err := p.GetSourceReadRowsCount()
			if err != nil {
				return 0, err
			}
			allCount += count
		}
	}

	return allCount, nil
}

func (p PostgresSource) GetDbTablesAccordingToSourceDbTables() (map[string][]string, error) {
	allDbTables := make(map[string][]string)
	for _, sourceDbTable := range p.cfg.SourceDbTables {
		dbTable := strings.Split(sourceDbTable, "@") // because `.` in regex is a special character, so use `@` to split
		if len(dbTable) != 2 {
			return nil, fmt.Errorf("invalid sourceDbTable: %s, should be a.b format", sourceDbTable)
		}
		dbs, err := p.GetDatabasesAccordingToSourceDbRegex(dbTable[0])
		if err != nil {
			return nil, fmt.Errorf("get databases according to sourceDbRegex failed: %v", err)
		}
		dbTables, err := p.GetTablesAccordingToSourceTableRegex(dbTable[1], dbs)
		if err != nil {
			return nil, fmt.Errorf("get tables according to sourceTableRegex failed: %v", err)
		}
		for db, tables := range dbTables {
			allDbTables[db] = append(allDbTables[db], tables...)
		}
	}
	return allDbTables, nil
}
