package source

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/databendcloud/db-archiver/config"
)

type SQLServerSource struct {
	db            *sql.DB
	cfg           *config.Config
	statsRecorder *DatabendSourceStatsRecorder
}

func NewSqlServerSource(cfg *config.Config) (*SQLServerSource, error) {
	stats := NewDatabendIntesterStatsRecorder()
	encodedPassword := url.QueryEscape(cfg.SourcePass)
	db, err := sql.Open("mssql", fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s&encrypt=disable",
		cfg.SourceUser,
		encodedPassword,
		cfg.SourceHost,
		cfg.SourcePort, cfg.SourceDB))
	if err != nil {
		logrus.Errorf("failed to open db: %v", err)
		return nil, err
	}
	return &SQLServerSource{
		db:            db,
		cfg:           cfg,
		statsRecorder: stats,
	}, nil
}

func (s *SQLServerSource) GetSourceReadRowsCount() (int, error) {
	// SQL Server table name contains schema，格式为 schema.table
	tableName := s.cfg.SourceTable
	if !strings.Contains(tableName, ".") {
		// if no schema，default use dbo schema
		tableName = "dbo." + tableName
	}

	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
	if s.cfg.SourceWhereCondition != "" {
		query += " WHERE " + s.cfg.SourceWhereCondition
	}

	row := s.db.QueryRow(query)
	var rowCount int
	err := row.Scan(&rowCount)
	if err != nil {
		return 0, err
	}

	return rowCount, nil
}

func (s *SQLServerSource) GetMinMaxSplitKey() (int64, int64, error) {
	tableName := s.cfg.SourceTable
	if !strings.Contains(tableName, ".") {
		tableName = "dbo." + tableName
	}

	query := fmt.Sprintf("SELECT MIN(%s) as min_key, MAX(%s) as max_key FROM %s.%s",
		s.cfg.SourceSplitKey,
		s.cfg.SourceSplitKey,
		s.cfg.SourceDB,
		tableName)

	if s.cfg.SourceWhereCondition != "" {
		query += " WHERE " + s.cfg.SourceWhereCondition
	}

	rows, err := s.db.Query(query)
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

	if err = rows.Err(); err != nil {
		return 0, 0, err
	}

	if !minSplitKey.Valid || !maxSplitKey.Valid {
		return 0, 0, nil
	}

	return minSplitKey.Int64, maxSplitKey.Int64, nil
}

func (s *SQLServerSource) AdjustBatchSizeAccordingToSourceDbTable() int64 {
	minSplitKey, maxSplitKey, err := s.GetMinMaxSplitKey()
	if err != nil {
		return s.cfg.BatchSize
	}
	sourceTableRowCount, err := s.GetSourceReadRowsCount()
	if err != nil {
		return s.cfg.BatchSize
	}
	rangeSize := maxSplitKey - minSplitKey + 1
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

func (s *SQLServerSource) GetMinMaxTimeSplitKey() (string, string, error) {
	parts := strings.Split(s.cfg.SourceTable, ".")
	var tableName string
	if len(parts) == 2 {
		tableName = fmt.Sprintf("[%s].[%s]", parts[0], parts[1])
	} else {
		tableName = fmt.Sprintf("[dbo].[%s]", s.cfg.SourceTable)
	}

	query := fmt.Sprintf(`
        SELECT 
            CONVERT(VARCHAR(23), MIN([%s]), 126) as min_key, 
            CONVERT(VARCHAR(23), MAX([%s]), 126) as max_key 
        FROM [%s].%s`,
		s.cfg.SourceSplitTimeKey,
		s.cfg.SourceSplitTimeKey,
		s.cfg.SourceDB,
		tableName)

	if s.cfg.SourceWhereCondition != "" {
		query += " WHERE " + s.cfg.SourceWhereCondition
	}

	rows, err := s.db.Query(query)
	if err != nil {
		return "", "", fmt.Errorf("executing query: %w", err)
	}
	defer rows.Close()

	var minSplitKey, maxSplitKey sql.NullString
	if rows.Next() {
		err = rows.Scan(&minSplitKey, &maxSplitKey)
		if err != nil {
			return "", "", fmt.Errorf("scanning results: %w", err)
		}
	} else {
		return "", "", fmt.Errorf("no results returned")
	}

	if err = rows.Err(); err != nil {
		return "", "", fmt.Errorf("reading rows: %w", err)
	}

	if !minSplitKey.Valid || !maxSplitKey.Valid {
		return "", "", nil
	}

	return minSplitKey.String, maxSplitKey.String, nil
}

func (s *SQLServerSource) DeleteAfterSync() error {
	if !s.cfg.DeleteAfterSync {
		return nil
	}

	parts := strings.Split(s.cfg.SourceTable, ".")
	var tableName string
	if len(parts) == 2 {
		tableName = fmt.Sprintf("[%s].[%s]", parts[0], parts[1])
	} else {
		tableName = fmt.Sprintf("[dbo].[%s]", s.cfg.SourceTable)
	}

	query := fmt.Sprintf("DELETE FROM [%s].%s",
		s.cfg.SourceDB,
		tableName)

	if s.cfg.SourceWhereCondition != "" {
		query += " WHERE " + s.cfg.SourceWhereCondition
	}

	_, err := s.db.Exec(query)
	if err != nil {
		return fmt.Errorf("executing delete query: %w", err)
	}

	return nil
}

func (s *SQLServerSource) QueryTableData(threadNum int, conditionSql string) ([][]interface{}, []string, error) {
	startTime := time.Now()

	parts := strings.Split(s.cfg.SourceTable, ".")
	var tableName string
	if len(parts) == 2 {
		tableName = fmt.Sprintf("[%s].[%s]", parts[0], parts[1])
	} else {
		tableName = fmt.Sprintf("[dbo].[%s]", s.cfg.SourceTable)
	}

	baseQuery := fmt.Sprintf(`
        SELECT TOP 1 * 
        FROM [%s].%s WITH (NOLOCK)
        WHERE %s`,
		s.cfg.SourceDB,
		tableName,
		strings.Split(conditionSql, "OFFSET")[0])

	if s.cfg.SourceWhereCondition != "" && s.cfg.SourceSplitKey != "" {
		baseQuery = fmt.Sprintf("%s AND %s", baseQuery, s.cfg.SourceWhereCondition)
	}

	rows, err := s.db.Query(baseQuery)
	if err != nil {
		return nil, nil, fmt.Errorf("executing base query: %w", err)
	}

	columns, err := rows.Columns()
	if err != nil {
		rows.Close()
		return nil, nil, fmt.Errorf("getting columns: %w", err)
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		rows.Close()
		return nil, nil, fmt.Errorf("getting column types: %w", err)
	}
	rows.Close()

	// scan value
	scanArgs := make([]interface{}, len(columns))
	for i, columnType := range columnTypes {
		switch columnType.DatabaseTypeName() {
		case "TINYINT", "SMALLINT", "INT", "BIGINT":
			scanArgs[i] = new(sql.NullInt64)
		case "REAL", "FLOAT":
			scanArgs[i] = new(sql.NullFloat64)
		case "DECIMAL", "NUMERIC", "MONEY", "SMALLMONEY":
			scanArgs[i] = new(sql.NullFloat64)
		case "CHAR", "VARCHAR", "TEXT", "NCHAR", "NVARCHAR", "NTEXT":
			scanArgs[i] = new(sql.NullString)
		case "DATE", "TIME", "DATETIME", "DATETIME2", "SMALLDATETIME", "DATETIMEOFFSET":
			scanArgs[i] = new(sql.NullString)
		case "BIT":
			scanArgs[i] = new(sql.NullBool)
		case "BINARY", "VARBINARY", "IMAGE":
			scanArgs[i] = new(sql.RawBytes)
		case "UNIQUEIDENTIFIER":
			scanArgs[i] = new(sql.NullString)
		default:
			scanArgs[i] = new(sql.RawBytes)
		}
	}

	const batchSize = 10000
	var result [][]interface{}
	offset := 0

	for {
		query := fmt.Sprintf(`
            SELECT *
            FROM [%s].%s WITH (NOLOCK)
            WHERE %s`,
			s.cfg.SourceDB,
			tableName,
			conditionSql)

		if s.cfg.SourceWhereCondition != "" && s.cfg.SourceSplitKey != "" {
			query = fmt.Sprintf("%s AND %s", query, s.cfg.SourceWhereCondition)
		}

		// page
		query = fmt.Sprintf(`
            SELECT *
            FROM (
                %s
            ) AS t
            ORDER BY (SELECT NULL)
            OFFSET %d ROWS
            FETCH NEXT %d ROWS ONLY`,
			query,
			offset,
			batchSize)

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		rows, err := s.db.QueryContext(ctx, query)

		if err != nil {
			return nil, nil, fmt.Errorf("executing batch query at offset %d: %w", offset, err)
		}

		rowCount := 0
		for rows.Next() {
			err = rows.Scan(scanArgs...)
			if err != nil {
				rows.Close()
				return nil, nil, fmt.Errorf("scanning row at offset %d: %w", offset, err)
			}

			row := make([]interface{}, len(columns))
			for i, v := range scanArgs {
				switch v := v.(type) {
				case *sql.NullString:
					if v.Valid {
						row[i] = v.String
					} else {
						row[i] = nil
					}
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
						if v.Bool {
							row[i] = 1 // target databend bool is int8
						} else {
							row[i] = 0
						}
					} else {
						row[i] = nil
					}
				case *sql.RawBytes:
					if v != nil {
						row[i] = string(*v)
					} else {
						row[i] = nil
					}
				default:
					row[i] = v
				}
			}
			result = append(result, row)
			rowCount++
		}

		rows.Close()

		if err = rows.Err(); err != nil {
			return nil, nil, fmt.Errorf("reading rows at offset %d: %w", offset, err)
		}

		// if the number of rows returned is less than the batch size, we can assume that we've reached the end of the data
		if rowCount < batchSize {
			break
		}

		offset += batchSize

		log.Printf("thread-%d: processed %d rows so far", threadNum, len(result))
	}

	s.statsRecorder.RecordMetric(len(result))
	stats := s.statsRecorder.Stats(time.Since(startTime))
	log.Printf("thread-%d: extract total %d rows (%f rows/s)", threadNum, len(result), stats.RowsPerSecondd)

	return result, columns, nil
}

func (s *SQLServerSource) GetDatabasesAccordingToSourceDbRegex(sourceDatabasePattern string) ([]string, error) {
	// SQL Server use system view to get databases
	query := `
        SELECT name 
        FROM sys.databases 
        WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')
        AND state_desc = 'ONLINE'
        AND HAS_DBACCESS(name) = 1
        ORDER BY name`

	rows, err := s.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("querying databases: %w", err)
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var database string
		err = rows.Scan(&database)
		if err != nil {
			return nil, fmt.Errorf("scanning database name: %w", err)
		}

		match, err := regexp.MatchString(sourceDatabasePattern, database)
		if err != nil {
			return nil, fmt.Errorf("matching pattern: %w", err)
		}

		if match {
			databases = append(databases, database)
		}
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("reading rows: %w", err)
	}

	return databases, nil
}

func (s *SQLServerSource) GetTablesAccordingToSourceTableRegex(sourceTablePattern string, databases []string) (map[string][]string, error) {
	dbTables := make(map[string][]string)

	baseQuery := `
        SELECT 
            SCHEMA_NAME(schema_id) as schema_name,
            name as table_name
        FROM sys.tables 
        WHERE type = 'U' 
        AND is_ms_shipped = 0
        ORDER BY schema_name, name`

	for _, database := range databases {
		// switch db
		_, err := s.db.Exec(fmt.Sprintf("USE [%s]", database))
		if err != nil {
			return nil, fmt.Errorf("switching to database %s: %w", database, err)
		}

		rows, err := s.db.Query(baseQuery)
		if err != nil {
			return nil, fmt.Errorf("querying tables in database %s: %w", database, err)
		}

		var tables []string
		for rows.Next() {
			var schemaName, tableName string
			err = rows.Scan(&schemaName, &tableName)
			if err != nil {
				rows.Close()
				return nil, fmt.Errorf("scanning table info in database %s: %w", database, err)
			}

			// schema.tablename, example: dbo.table1
			fullTableName := fmt.Sprintf("%s.%s", schemaName, tableName)
			fmt.Println("full name table:", fullTableName)

			match, err := regexp.MatchString(sourceTablePattern, fullTableName)
			if err != nil {
				rows.Close()
				return nil, fmt.Errorf("matching pattern for table %s: %w", fullTableName, err)
			}

			if match {
				fmt.Println("match table:", fullTableName)
				tables = append(tables, fullTableName)
			}
		}

		rows.Close()
		if err = rows.Err(); err != nil {
			return nil, fmt.Errorf("reading rows for database %s: %w", database, err)
		}

		dbTables[database] = tables
	}

	fmt.Println("dbTables:", dbTables)

	return dbTables, nil
}

func (s *SQLServerSource) GetAllSourceReadRowsCount() (int, error) {
	allCount := 0

	dbTables, err := s.GetDbTablesAccordingToSourceDbTables()
	if err != nil {
		return 0, fmt.Errorf("getting database tables: %w", err)
	}

	for db, tables := range dbTables {
		_, err := s.db.Exec(fmt.Sprintf("USE [%s]", db))
		if err != nil {
			return 0, fmt.Errorf("switching to database %s: %w", db, err)
		}

		s.cfg.SourceDB = db
		for _, table := range tables {
			// 解析 schema 和表名
			parts := strings.Split(table, ".")
			if len(parts) != 2 {
				return 0, fmt.Errorf("invalid table name format for %s, expected schema.table", table)
			}
			s.cfg.SourceTable = table

			count, err := s.GetSourceReadRowsCount()
			if err != nil {
				return 0, fmt.Errorf("getting row count for %s.%s: %w", db, table, err)
			}
			allCount += count
		}
	}

	if allCount == 0 && len(dbTables) == 0 && s.cfg.SourceTable != "" {
		count, err := s.GetSourceReadRowsCount()
		if err != nil {
			return 0, fmt.Errorf("getting row count for single table %s: %w", s.cfg.SourceTable, err)
		}
		allCount += count
	}

	return allCount, nil
}

func (s *SQLServerSource) GetDbTablesAccordingToSourceDbTables() (map[string][]string, error) {
	allDbTables := make(map[string][]string)

	for _, sourceDbTable := range s.cfg.SourceDbTables {
		dbTable := strings.Split(sourceDbTable, "@")
		if len(dbTable) != 2 {
			return nil, fmt.Errorf("invalid sourceDbTable: %s, should be database@schema.table format", sourceDbTable)
		}

		dbs, err := s.GetDatabasesAccordingToSourceDbRegex(dbTable[0])
		if err != nil {
			return nil, fmt.Errorf("get databases according to sourceDbRegex failed: %w", err)
		}

		if len(dbs) == 0 {
			log.Printf("Warning: No databases match pattern %s", dbTable[0])
			continue
		}

		// match table
		dbTables, err := s.GetTablesAccordingToSourceTableRegex(dbTable[1], dbs)
		if err != nil {
			return nil, fmt.Errorf("get tables according to sourceTableRegex failed: %w", err)
		}

		for db, tables := range dbTables {
			if existingTables, ok := allDbTables[db]; ok {
				tableSet := make(map[string]struct{})
				for _, t := range existingTables {
					tableSet[t] = struct{}{}
				}

				for _, t := range tables {
					if _, exists := tableSet[t]; !exists {
						allDbTables[db] = append(allDbTables[db], t)
					}
				}
			} else {
				allDbTables[db] = tables
			}
		}
	}

	return allDbTables, nil
}
