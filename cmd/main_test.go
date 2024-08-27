package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/test-go/testify/assert"

	cfg "github.com/databendcloud/db-archiver/config"
	"github.com/databendcloud/db-archiver/ingester"
	"github.com/databendcloud/db-archiver/source"
	"github.com/databendcloud/db-archiver/worker"

	_ "github.com/datafuselabs/databend-go"
)

func TestMultipleDbTablesWorkflow(t *testing.T) {

	prepareDbxTablex()
	prepareDatabend()

	testConfig := prepareMultipleConfig()
	startTime := time.Now()

	ig := ingester.NewDatabendIngester(testConfig)
	src, err := source.NewSource(testConfig)
	assert.NoError(t, err)
	wg := sync.WaitGroup{}
	wg.Add(1)
	dbTables, err := src.GetDbTablesAccordingToSourceDbTables()
	assert.NoError(t, err)
	for db, tables := range dbTables {
		for _, table := range tables {
			w := worker.NewWorker(testConfig, db, table, fmt.Sprintf("%s.%s", db, table), ig, src)
			go func() {
				w.Run(context.Background())
				wg.Done()
			}()
		}
	}
	wg.Wait()
	endTime := fmt.Sprintf("end time: %s", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Println(endTime)
	fmt.Println(fmt.Sprintf("total time: %s", time.Since(startTime)))
	checkTargetTable()
}

func TestWorkFlow(t *testing.T) {
	prepareMysql()
	prepareDatabend()
	testConfig := prepareTestConfig()
	startTime := time.Now()

	ig := ingester.NewDatabendIngester(testConfig)
	src, err := source.NewSource(testConfig)
	if err != nil {
		panic(err)
	}
	wg := sync.WaitGroup{}
	wg.Add(1)
	dbs, err := src.GetDatabasesAccordingToSourceDbRegex(testConfig.SourceDB)
	if err != nil {
		panic(err)
	}
	dbTables, err := src.GetTablesAccordingToSourceTableRegex(testConfig.SourceTable, dbs)
	if err != nil {
		panic(err)
	}
	for db, tables := range dbTables {
		for _, table := range tables {
			w := worker.NewWorker(testConfig, db, table, fmt.Sprintf("%s.%s", db, table), ig, src)
			go func() {
				w.Run(context.Background())
				wg.Done()
			}()
		}
	}
	wg.Wait()
	endTime := fmt.Sprintf("end time: %s", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Println(endTime)
	fmt.Println(fmt.Sprintf("total time: %s", time.Since(startTime)))

	checkTargetTable()
}

func prepareDbxTablex() {
	db, err := sql.Open("mysql", "root:123456@tcp(127.0.0.1:3306)/mysql")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	db.Exec("create database if not exists db1")
	db.Exec("create database if not exists db2")
	db.Exec(`
CREATE TABLE db1.test_table1 (
	id BIGINT UNSIGNED PRIMARY KEY,
	int_col INT,
	varchar_col VARCHAR(255),
	float_col FLOAT,
	bool_col BOOL,
	de decimal(18,6),
	date_col DATE,
	datetime_col DATETIME,
	timestamp_col TIMESTAMP
)
`)
	db.Exec(`
CREATE TABLE db2.test_table2 (
    	id BIGINT UNSIGNED PRIMARY KEY,
    		int_col INT,
    		varchar_col VARCHAR(255),
    		float_col FLOAT,
    		bool_col BOOL,
    		de decimal(18,6),
    		date_col DATE,
    		datetime_col DATETIME,
    		timestamp_col TIMESTAMP
	)
`)
	for i := 1; i <= 10; i++ {
		_, err = db.Exec(`
			INSERT INTO db1.test_table1
			(id, int_col, varchar_col, float_col, de, bool_col, date_col,  datetime_col, timestamp_col) 
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, i, i, fmt.Sprintf("varchar %d", i), float64(i), i%2 == 0, 1.1, "2022-01-01", "2022-01-01 00:00:00", "2024-06-30 20:00:00")
		if err != nil {
			log.Fatal(err)
		}
	}

	for i := 1; i <= 10; i++ {
		_, err = db.Exec(`
			INSERT INTO db2.test_table2
			(id, int_col, varchar_col, float_col, de, bool_col, date_col,  datetime_col, timestamp_col) 
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, i, i, fmt.Sprintf("varchar %d", i), float64(i), i%2 == 0, 1.1, "2022-01-01", "2022-01-01 00:00:00", "2024-06-30 20:00:00")
		if err != nil {
			log.Fatal(err)
		}
	}
}

func prepareMysql() {
	db, err := sql.Open("mysql", "root:123456@tcp(127.0.0.1:3306)/mysql")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	db.Exec("Create database if not exists mydb")
	db.Exec("Use mydb")

	// Create table
	_, err = db.Exec(`
		CREATE TABLE mydb.test_table (
			id BIGINT UNSIGNED PRIMARY KEY,
			int_col INT,
			varchar_col VARCHAR(255),
			float_col FLOAT,
			bool_col BOOL,
			de decimal(18,6),
			date_col DATE,
			datetime_col DATETIME,
			timestamp_col TIMESTAMP
		)
	`)
	// need to test the TIME type in mysql
	if err != nil {
		log.Fatal(err)
	}

	// Insert data
	for i := 1; i <= 10; i++ {
		_, err = db.Exec(`
			INSERT INTO mydb.test_table 
			(id, int_col, varchar_col, float_col, de, bool_col, date_col,  datetime_col, timestamp_col) 
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, i, i, fmt.Sprintf("varchar %d", i), float64(i), i%2 == 0, 1.1, "2022-01-01", "2022-01-01 00:00:00", "2024-06-30 20:00:00")
		if err != nil {
			log.Fatal(err)
		}
	}

	for i := 1; i <= 10; i++ {
		var intCol sql.NullInt64
		var varcharCol sql.NullString
		var timeCol sql.NullTime
		if i%2 == 0 {
			intCol = sql.NullInt64{Int64: int64(i), Valid: true}
			varcharCol = sql.NullString{String: fmt.Sprintf("varchar %d", i), Valid: true}
			timeCol = sql.NullTime{Time: time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC), Valid: true}
		} else {
			intCol = sql.NullInt64{Valid: false}
			varcharCol = sql.NullString{Valid: false}
			timeCol = sql.NullTime{Valid: false}
		}
		_, err = db.Exec(`
		INSERT INTO mydb.test_table 
		(id, int_col, varchar_col, float_col, de, bool_col, date_col,  datetime_col, timestamp_col) 
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, i*11, intCol, varcharCol, float64(i), i%2 == 0, 1.1, "2022-01-01", "2022-01-01 00:00:00", timeCol)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func prepareDatabend() {
	db, err := sql.Open("databend", "http://databend:databend@localhost:8000")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(`
		CREATE TABLE if not exists test_table (
			id UINT64,
			int_col INT,
			varchar_col VARCHAR(255),
			float_col FLOAT,
			bool_col TINYINT,
			de decimal(18,6),
			date_col DATE,
			datetime_col TIMESTAMP,
			timestamp_col TIMESTAMP
		)
	`)
	if err != nil {
		log.Fatal(err)
	}
}

func prepareTestConfig() *cfg.Config {
	config := cfg.Config{
		SourceDB:             "mydb",
		SourceHost:           "127.0.0.1",
		SourcePort:           3306,
		SourceUser:           "root",
		SourcePass:           "123456",
		SourceTable:          "test_table",
		SourceWhereCondition: "id > 0",
		SourceQuery:          "select * from mydb.test_table",
		SourceSplitKey:       "id",
		SourceSplitTimeKey:   "",
		DatabendDSN:          "http://databend:databend@localhost:8000",
		DatabendTable:        "default.test_table",
		BatchSize:            5,
		BatchMaxInterval:     3,
		MaxThread:            2,
		CopyForce:            false,
		CopyPurge:            false,
		DeleteAfterSync:      false,
		DisableVariantCheck:  false,
		UserStage:            "~",
	}

	return &config
}

func prepareMultipleConfig() *cfg.Config {
	config := cfg.Config{
		SourceDB:             "mydb",
		SourceHost:           "127.0.0.1",
		SourcePort:           3306,
		SourceUser:           "root",
		SourcePass:           "123456",
		SourceDbTables:       []string{"db*.test_table*"},
		SourceTable:          "test_table",
		SourceWhereCondition: "id > 0",
		SourceQuery:          "select * from default.test_table",
		SourceSplitKey:       "id",
		SourceSplitTimeKey:   "",
		DatabendDSN:          "http://databend:databend@localhost:8000",
		DatabendTable:        "default.test_table",
		BatchSize:            5,
		BatchMaxInterval:     3,
		MaxThread:            2,
		CopyForce:            false,
		CopyPurge:            false,
		DeleteAfterSync:      false,
		DisableVariantCheck:  false,
		UserStage:            "~",
	}

	return &config
}

func checkTargetTable() {
	db, err := sql.Open("databend", "http://databend:databend@localhost:8000")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query(`
		SELECT * FROM default.test_table
	`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	count := 0

	for rows.Next() {
		var id int
		var int_col interface{}
		var varchar_col string
		var float_col float64
		var bool_col bool
		var de float64
		var date_col string
		var datetime_col string
		var timestamp_col string
		err = rows.Scan(&id, &int_col, &varchar_col, &float_col, &bool_col, &de, &date_col, &datetime_col, &timestamp_col)
		if err != nil {
			log.Fatal(err)
		}
		count += 1
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	defer db.Close()
	fmt.Println("target table count: ", count)
	if count != 20 {
		panic("target table count not equal 10")
	}
}
