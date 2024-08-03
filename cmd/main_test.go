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

	cfg "github.com/databendcloud/db-archiver/config"
	"github.com/databendcloud/db-archiver/ingester"
	"github.com/databendcloud/db-archiver/source"
	"github.com/databendcloud/db-archiver/worker"
)

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
	w := worker.NewWorker(testConfig, fmt.Sprintf("worker"), ig, src)
	go func() {
		w.Run(context.TODO())
		wg.Done()
	}()
	wg.Wait()
	endTime := fmt.Sprintf("end time: %s", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Println(endTime)
	fmt.Println(fmt.Sprintf("total time: %s", time.Since(startTime)))

	checkTargetTable()
}

func prepareMysql() {
	db, err := sql.Open("mysql", "root:123456@tcp(127.0.0.1:3306)/default")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(`
		CREATE TABLE test_table (
			id BIGINT UNSIGNED PRIMARY KEY,
			int_col INT,
			varchar_col VARCHAR(255),
			float_col FLOAT,
			bool_col BOOL,
			de decimal(18,6),
			date_col DATE,
			time_col TIMESTAMP,
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
			INSERT INTO test_table 
			(id, int_col, varchar_col, float_col, de, bool_col, date_col, time_col, datetime_col, timestamp_col) 
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, i, i, fmt.Sprintf("varchar %d", i), float64(i), i%2 == 0, 1.1, "2022-01-01", "2022-01-01 00:00:00", "2022-01-01 00:00:00", "2022-01-01 00:00:00")
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
		CREATE TABLE test_table (
			id UINT64,
			int_col INT,
			varchar_col VARCHAR(255),
			float_col FLOAT,
			bool_col TINYINT,
			de decimal(18,6),
			date_col DATE,
			time_col TIMESTAMP,
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
		SourceDB:             "default",
		SourceHost:           "127.0.0.1",
		SourcePort:           3306,
		SourceUser:           "root",
		SourcePass:           "123456",
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
		var int_col int
		var varchar_col string
		var float_col float64
		var bool_col bool
		var de float64
		var date_col string
		var time_col string
		var datetime_col string
		var timestamp_col string
		err = rows.Scan(&id, &int_col, &varchar_col, &float_col, &bool_col, &de, &date_col, &time_col, &datetime_col, &timestamp_col)
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
	if count != 10 {
		panic("target table count not equal 10")
	}
}
