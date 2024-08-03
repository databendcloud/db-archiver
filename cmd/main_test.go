package main

import (
	"database/sql"
	"fmt"
	"log"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

func TestWorkFlow(t *testing.T) {
	prepareMysql()
	prepareDatabend()
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
			time_col TIME,
			datetime_col DATETIME,
			timestamp_col TIMESTAMP
		)
	`)
	if err != nil {
		log.Fatal(err)
	}

	// Insert data
	for i := 1; i <= 10; i++ {
		_, err = db.Exec(`
			INSERT INTO test_table 
			(id, int_col, varchar_col, float_col, de, bool_col, date_col, time_col, datetime_col, timestamp_col) 
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, i, i, fmt.Sprintf("varchar %d", i), float64(i), i%2 == 0, 1.1, "2022-01-01", "00:00:00", "2022-01-01 00:00:00", "2022-01-01 00:00:00")
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
			id BIGINT UNSIGNED PRIMARY KEY,
			int_col INT,
			varchar_col VARCHAR(255),
			float_col FLOAT,
			bool_col BOOL,
			de decimal(18,6),
			date_col DATE,
			time_col TIME,
			datetime_col DATETIME,
			timestamp_col TIMESTAMP
		)
	`)
	if err != nil {
		log.Fatal(err)
	}
}
