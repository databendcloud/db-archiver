package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/test-go/testify/assert"

	cfg "github.com/databendcloud/db-archiver/config"
	"github.com/databendcloud/db-archiver/ingester"
	"github.com/databendcloud/db-archiver/source"
	"github.com/databendcloud/db-archiver/utils/testutils"
	"github.com/databendcloud/db-archiver/worker"
)

const testPostgresPort = 15432

func TestMultiplePgTable(t *testing.T) {
	dsn, tearDown := testutils.PostgresForTest()
	defer tearDown()
	preparePgDbxTablex(dsn)
	prepareDatabend("test_table3", "https://dbarchiver:abc123@tn3ftqihs--medium-p8at.gw.aws-us-east-2.default.databend.com:443")

	testConfig := preparePGMultipleConfig()
	startTime := time.Now()

	src, err := source.NewSource(testConfig)
	assert.NoError(t, err)

	dbTables, err := src.GetDbTablesAccordingToSourceDbTables()
	assert.NoError(t, err)
	for db, tables := range dbTables {
		for _, table := range tables {
			db := db
			table := table
			cfgCopy := *testConfig
			cfgCopy.SourceDB = db
			cfgCopy.SourceTable = table
			ig := ingester.NewDatabendIngester(&cfgCopy)
			src, err := source.NewSource(&cfgCopy)
			assert.NoError(t, err)
			w := worker.NewWorker(&cfgCopy, fmt.Sprintf("%s.%s", db, table), ig, src)
			w.Run(context.Background())
		}
	}
	endTime := fmt.Sprintf("end time: %s", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Println(endTime)
	fmt.Println(fmt.Sprintf("total time: %s", time.Since(startTime)))
	err = checkTargetTable("test_table3", 15)
	assert.NoError(t, err)
}

func preparePgDbxTablex(dsn string) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	db.Exec(`
CREATE TABLE test_table1 (
	id BIGINT PRIMARY KEY,
	int_col INT,
	varchar_col VARCHAR(255),
	float_col FLOAT,
	bool_col BOOL,
	de decimal(18,6),
	date_col DATE,
	datetime_col TIMESTAMP,
	timestamp_col TIMESTAMP
)
`)
	for i := 1; i <= 10; i++ {
		deValue := 0
		if i%2 == 0 {
			deValue = 1
		}
		_, err = db.Exec(`
 INSERT INTO test_table1
 (id, int_col, varchar_col, float_col, bool_col, de, date_col, datetime_col, timestamp_col)
 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
`, i, i, fmt.Sprintf("varchar %d", i), float64(i), deValue, 1.1, "2022-01-01", "2022-01-01 00:00:00", "2024-06-30 20:00:00")
		if err != nil {
			log.Fatal(err)
		}
	}

	db.Exec("create database mydb1")
	dsn = fmt.Sprintf("postgres://postgres:postgres@localhost:%d/mydb1?sslmode=disable", testPostgresPort)

	db.Exec(`
CREATE TABLE test_table2 (
	id BIGINT PRIMARY KEY,
	int_col INT,
	varchar_col VARCHAR(255),
	float_col FLOAT,
	bool_col BOOL,
	de decimal(18,6),
	date_col DATE,
	datetime_col TIMESTAMP,
	timestamp_col TIMESTAMP
)
`)

	for i := 1; i <= 5; i++ {
		deValue := 0
		if i%2 == 0 {
			deValue = 1
		}
		_, err = db.Exec(`
 INSERT INTO test_table2
 (id, int_col, varchar_col, float_col, bool_col, de, date_col,  datetime_col, timestamp_col)
 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
`, i+1, i, fmt.Sprintf("varchar %d", i), float64(i), deValue, 1.1, "2022-01-01", "2022-01-01 00:00:00", "2024-06-30 20:00:00")
		if err != nil {
			log.Fatal(err)
		}
	}
}

func preparePGMultipleConfig() *cfg.Config {
	config := cfg.Config{
		SourceDB:             "mydb",
		SourceHost:           "127.0.0.1",
		SourcePort:           testPostgresPort,
		SourceUser:           "postgres",
		SourcePass:           "postgres",
		SourceDbTables:       []string{"mydb.*@test_table.*"},
		SourceTable:          "test_table",
		SourceWhereCondition: "id > 0",
		DatabaseType:         "pg",
		SourceQuery:          "select * from mydb2.test_table",
		SourceSplitKey:       "id",
		SourceSplitTimeKey:   "",
		DatabendDSN:          "https://dbarchiver:abc123@tn3ftqihs--medium-p8at.gw.aws-us-east-2.default.databend.com:443",
		DatabendTable:        "default.test_table3",
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
