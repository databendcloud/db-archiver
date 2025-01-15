package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/url"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	go_ora "github.com/sijms/go-ora/v2"
	"github.com/test-go/testify/assert"

	cfg "github.com/databendcloud/db-archiver/config"
	"github.com/databendcloud/db-archiver/ingester"
	"github.com/databendcloud/db-archiver/source"
	"github.com/databendcloud/db-archiver/worker"

	_ "github.com/datafuselabs/databend-go"
)

func TestMultipleDbTablesWorkflow(t *testing.T) {
	{
		fmt.Println("=== TEST MYSQL SOURCE ===")
		prepareMySQLDbxTablex()
		prepareDatabend("test_table2", "http://databend:databend@localhost:8000")

		testConfig := prepareMySQLMultipleConfig()
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
		err = checkTargetTable("test_table2", 15)
		assert.NoError(t, err)
	}

}

func TestOracleMultiTableWorkflow(t *testing.T) {
	t.Skip("skip test")
	fmt.Println("=== TEST ORACLE SOURCE ===")
	prepareOracleDbxTablex()
	truncateDatabend("test_table2", "http://databend:databend@localhost:8000")
	prepareDatabend("test_table2", "http://databend:databend@localhost:8000")

	testConfig := prepareOracleMultipleConfig()
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
	err = checkTargetTable("test_table2", 15)
	assert.NoError(t, err)
}

func TestMySQLWorkFlow(t *testing.T) {
	{
		fmt.Println("=== TEST MYSQL SOURCE ===")
		prepareMysql()
		prepareDatabend("test_table", "http://databend:databend@localhost:8000")
		testConfig := prepareTestConfig()
		startTime := time.Now()

		src, err := source.NewSource(testConfig)
		if err != nil {
			panic(err)
		}
		wg := sync.WaitGroup{}
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
				wg.Add(1)
				db := db
				table := table
				go func(cfg *cfg.Config, db, table string) {
					cfgCopy := *testConfig
					cfgCopy.SourceTable = table
					cfgCopy.SourceDB = db
					ig := ingester.NewDatabendIngester(&cfgCopy)
					src, err := source.NewSource(&cfgCopy)
					assert.NoError(t, err)
					w := worker.NewWorker(&cfgCopy, fmt.Sprintf("%s.%s", db, table), ig, src)
					w.Run(context.Background())
					wg.Done()
				}(testConfig, db, table)
			}
		}
		wg.Wait()
		endTime := fmt.Sprintf("end time: %s", time.Now().Format("2006-01-02 15:04:05"))
		fmt.Println(endTime)
		fmt.Println(fmt.Sprintf("total time: %s", time.Since(startTime)))

		err = checkTargetTable("test_table", 20)
		assert.NoError(t, err)
	}
}

func TestMssqlWorkflow(t *testing.T) {
	fmt.Println("=== TEST MSSQL SOURCE ===")
	prepareSQLServer()
	truncateDatabend("test_table", "http://databend:databend@localhost:8000")
	prepareDatabend("test_table", "http://databend:databend@localhost:8000")
	testConfig := prepareSqlServerTestConfig()
	startTime := time.Now()

	src, err := source.NewSource(testConfig)
	if err != nil {
		panic(err)
	}
	wg := sync.WaitGroup{}
	dbs, err := src.GetDatabasesAccordingToSourceDbRegex(testConfig.SourceDB)
	if err != nil {
		panic(err)
	}
	log.Printf("dbs: %v", dbs)
	dbTables, err := src.GetTablesAccordingToSourceTableRegex(testConfig.SourceTable, dbs)
	if err != nil {
		panic(err)
	}
	log.Printf("dbTables: %v", dbTables)
	for db, tables := range dbTables {
		for _, table := range tables {
			wg.Add(1)
			db := db
			table := table
			go func(cfg *cfg.Config, db, table string) {
				cfgCopy := *testConfig
				cfgCopy.SourceTable = table
				cfgCopy.SourceDB = db
				ig := ingester.NewDatabendIngester(&cfgCopy)
				src, err := source.NewSource(&cfgCopy)
				assert.NoError(t, err)
				w := worker.NewWorker(&cfgCopy, fmt.Sprintf("%s.%s", db, table), ig, src)
				w.Run(context.Background())
				wg.Done()
			}(testConfig, db, table)
		}
	}
	wg.Wait()
	endTime := fmt.Sprintf("end time: %s", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Println(endTime)
	fmt.Println(fmt.Sprintf("total time: %s", time.Since(startTime)))

	err = checkTargetTable("test_table", 10)
	assert.NoError(t, err)
}

func TestMssqlTimeKeyWorkflow(t *testing.T) {
	fmt.Println("=== TEST MSSQL SOURCE WITH TIME KEY ===")
	prepareSQLServer()
	truncateDatabend("test_table", "http://databend:databend@localhost:8000")
	prepareDatabend("test_table", "http://databend:databend@localhost:8000")
	testConfig := prepareSqlServerTimeKeyTestConfig()
	startTime := time.Now()

	src, err := source.NewSource(testConfig)
	if err != nil {
		panic(err)
	}
	wg := sync.WaitGroup{}
	dbs, err := src.GetDatabasesAccordingToSourceDbRegex(testConfig.SourceDB)
	if err != nil {
		panic(err)
	}
	log.Printf("dbs: %v", dbs)
	dbTables, err := src.GetTablesAccordingToSourceTableRegex(testConfig.SourceTable, dbs)
	if err != nil {
		panic(err)
	}
	log.Printf("dbTables: %v", dbTables)
	for db, tables := range dbTables {
		for _, table := range tables {
			wg.Add(1)
			db := db
			table := table
			go func(cfg *cfg.Config, db, table string) {
				cfgCopy := *testConfig
				cfgCopy.SourceTable = table
				cfgCopy.SourceDB = db
				ig := ingester.NewDatabendIngester(&cfgCopy)
				src, err := source.NewSource(&cfgCopy)
				assert.NoError(t, err)
				w := worker.NewWorker(&cfgCopy, fmt.Sprintf("%s.%s", db, table), ig, src)
				w.Run(context.Background())
				wg.Done()
			}(testConfig, db, table)
		}
	}
	wg.Wait()
	endTime := fmt.Sprintf("end time: %s", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Println(endTime)
	fmt.Println(fmt.Sprintf("total time: %s", time.Since(startTime)))

	err = checkTargetTable("test_table", 10)
	assert.NoError(t, err)
}

func TestSimpleOracleWorkflow(t *testing.T) {
	t.Skip("skip test")
	fmt.Println("=== TEST ORACLE SOURCE ===")
	prepareOracle()
	truncateDatabend("test_table", "http://databend:databend@localhost:8000")
	prepareDatabend("test_table", "http://databend:databend@localhost:8000")
	testConfig := prepareOracleTestConfig()
	startTime := time.Now()

	src, err := source.NewSource(testConfig)
	if err != nil {
		panic(err)
	}
	wg := sync.WaitGroup{}
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
			wg.Add(1)
			db := db
			table := table
			go func(cfg *cfg.Config, db, table string) {
				cfgCopy := *testConfig
				cfgCopy.SourceTable = table
				cfgCopy.SourceDB = db
				ig := ingester.NewDatabendIngester(&cfgCopy)
				src, err := source.NewSource(&cfgCopy)
				assert.NoError(t, err)
				w := worker.NewWorker(&cfgCopy, fmt.Sprintf("%s.%s", db, table), ig, src)
				w.Run(context.Background())
				wg.Done()
			}(testConfig, db, table)
		}
	}
	wg.Wait()
	endTime := fmt.Sprintf("end time: %s", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Println(endTime)
	fmt.Println(fmt.Sprintf("total time: %s", time.Since(startTime)))

	// mydb1 10 rows , mydb2 5 rows, mydb 10 rows
	err = checkTargetTable("test_table", 25)
	assert.NoError(t, err)
}

func prepareMySQLDbxTablex() {
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

	for i := 1; i <= 5; i++ {
		_, err = db.Exec(`
			INSERT INTO db2.test_table2
			(id, int_col, varchar_col, float_col, de, bool_col, date_col,  datetime_col, timestamp_col) 
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, i+1, i, fmt.Sprintf("varchar %d", i), float64(i), i%2 == 0, 1.1, "2022-01-01", "2022-01-01 00:00:00", "2024-06-30 20:00:00")
		if err != nil {
			log.Fatal(err)
		}
	}
}

func prepareOracleDbxTablex() {
	server := "localhost"
	port := 49161
	serviceName := "XE"
	username := "a"
	password := "123"

	// 使用 go-ora 构建连接字符串
	connStr := go_ora.BuildUrl(server, port, serviceName, username, password, nil)

	// 打开数据库连接
	db, err := sql.Open("oracle", connStr)
	if err != nil {
		log.Fatalf("Failed to open connection: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("BEGIN EXECUTE IMMEDIATE 'DROP USER mydb1 CASCADE'; EXCEPTION WHEN OTHERS THEN IF SQLCODE = -1918 THEN NULL; ELSE RAISE; END IF; END;")
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec(fmt.Sprintf("CREATE USER mydb1 IDENTIFIED BY mydb DEFAULT TABLESPACE USERS"))
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(fmt.Sprintf("GRANT DBA TO mydb1"))
	if err != nil {
		log.Fatal(err)
	}

	db.Exec(`
CREATE TABLE mydb1.test_table1 (
	id NUMBER(10) PRIMARY KEY,
			int_col NUMBER(10),
			varchar_col VARCHAR2(255),
			float_col NUMBER,
			bool_col NUMBER(1) CHECK (bool_col IN (0, 1)),
			de NUMBER(18,6),
			date_col DATE,
			datetime_col TIMESTAMP,
			timestamp_col TIMESTAMP
)
`)

	_, err = db.Exec("BEGIN EXECUTE IMMEDIATE 'DROP USER mydb2 CASCADE'; EXCEPTION WHEN OTHERS THEN IF SQLCODE = -1918 THEN NULL; ELSE RAISE; END IF; END;")
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec(fmt.Sprintf("CREATE USER mydb2 IDENTIFIED BY mydb DEFAULT TABLESPACE USERS"))
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(fmt.Sprintf("GRANT DBA TO mydb2"))
	if err != nil {
		log.Fatal(err)
	}

	db.Exec(`
CREATE TABLE mydb2.test_table2 (
    	id NUMBER(10) PRIMARY KEY,
			int_col NUMBER(10),
			varchar_col VARCHAR2(255),
			float_col NUMBER,
			bool_col NUMBER(1) CHECK (bool_col IN (0, 1)),
			de NUMBER(18,6),
			date_col DATE,
			datetime_col TIMESTAMP,
			timestamp_col TIMESTAMP
	)
`)
	for i := 1; i <= 10; i++ {
		insert := fmt.Sprintf("INSERT INTO mydb1.test_table1 "+
			"(id, int_col, varchar_col, float_col, de, bool_col, date_col,  datetime_col, timestamp_col) "+
			"values (%d, %d, '%s', %f, %d, %f,%s,%s,%s)", i, i, fmt.Sprintf("varchar %d", i), float64(i), i%2, 1.1, "TO_DATE('2022-01-01', 'YYYY-MM-DD')", "TO_TIMESTAMP('2022-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')", "TO_TIMESTAMP('2024-06-30 20:00:00', 'YYYY-MM-DD HH24:MI:SS')")
		_, err = db.Exec(insert)
		if err != nil {
			log.Fatal(err)
		}
	}

	for i := 1; i <= 5; i++ {
		insert := fmt.Sprintf("INSERT INTO mydb2.test_table2 "+
			"(id, int_col, varchar_col, float_col, de, bool_col, date_col,  datetime_col, timestamp_col) "+
			"values (%d, %d, '%s', %f, %d, %f,%s,%s,%s)", i, i, fmt.Sprintf("varchar %d", i), float64(i), i%2, 1.1, "TO_DATE('2022-01-01', 'YYYY-MM-DD')", "TO_TIMESTAMP('2022-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')", "TO_TIMESTAMP('2024-06-30 20:00:00', 'YYYY-MM-DD HH24:MI:SS')")
		_, err = db.Exec(insert)
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
	db.Exec("drop table if exists mydb.test_table")

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

func prepareOracle() {
	server := "localhost"
	port := 49161
	serviceName := "XE"
	username := "a"
	password := "123"

	connStr := go_ora.BuildUrl(server, port, serviceName, username, password, nil)

	db, err := sql.Open("oracle", connStr)
	if err != nil {
		log.Fatalf("Failed to open connection: %v", err)
	}
	defer db.Close()
	_, err = db.Exec("BEGIN EXECUTE IMMEDIATE 'DROP USER mydb CASCADE'; EXCEPTION WHEN OTHERS THEN IF SQLCODE = -1918 THEN NULL; ELSE RAISE; END IF; END;")
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec(fmt.Sprintf("CREATE USER mydb IDENTIFIED BY mydb DEFAULT TABLESPACE USERS"))
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(fmt.Sprintf("GRANT DBA TO mydb"))
	if err != nil {
		log.Fatal(err)
	}

	// Create table
	_, err = db.Exec(`
		CREATE TABLE mydb.test_table (
			id NUMBER(10) PRIMARY KEY,
			int_col NUMBER(10),
			varchar_col VARCHAR2(255),
			float_col NUMBER,
			bool_col NUMBER(1) CHECK (bool_col IN (0, 1)),
			de NUMBER(18,6),
			date_col DATE,
			datetime_col TIMESTAMP,
			timestamp_col TIMESTAMP
		)
	`)

	if err != nil {
		log.Fatal(err)
	}

	// Insert data
	for i := 1; i <= 10; i++ {
		insert := fmt.Sprintf("INSERT INTO mydb.test_table "+
			"(id, int_col, varchar_col, float_col, de, bool_col, date_col,  datetime_col, timestamp_col) "+
			"values (%d, %d, '%s', %f, %d, %f,%s,%s,%s)", i, i, fmt.Sprintf("varchar %d", i), float64(i), i%2, 1.1, "TO_DATE('2022-01-01', 'YYYY-MM-DD')", "TO_TIMESTAMP('2022-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')", "TO_TIMESTAMP('2024-06-30 20:00:00', 'YYYY-MM-DD HH24:MI:SS')")
		_, err = db.Exec(insert)
		if err != nil {
			log.Fatal(err)
		}
	}

}

func prepareDatabend(tableName string, dsn string) {
	db, err := sql.Open("databend", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(fmt.Sprintf(
		`CREATE TABLE if not exists default.%s (
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
	`, tableName))
	if err != nil {
		log.Fatal(err)
	}
}

func truncateDatabend(tableName string, dsn string) {
	db, err := sql.Open("databend", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// TRUNCATE TABLE
	_, err = db.Exec(fmt.Sprintf(
		`TRUNCATE TABLE default.%s
	`, tableName))
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

func prepareSQLServer() {
	log.Println("===prepareSQLServer===")
	encodedPassword := url.QueryEscape("Password1234!")
	// sqlserver://username:password@host:port?database=dbname
	db, err := sql.Open("mssql", fmt.Sprintf("sqlserver://sa:%s@localhost:1433?encrypt=disable", encodedPassword))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// delete database
	_, err = db.Exec(`
        IF EXISTS (SELECT * FROM sys.databases WHERE name = 'mydb')
        BEGIN
            ALTER DATABASE mydb SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
            DROP DATABASE mydb;
        END
    `)
	if err != nil {
		log.Fatal(err)
	}

	// new database
	_, err = db.Exec("CREATE DATABASE mydb")
	if err != nil {
		log.Fatal(err)
	}

	db, err = sql.Open("mssql", fmt.Sprintf("sqlserver://sa:%s@localhost:1433?database=mydb&encrypt=disable", encodedPassword))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// create table
	_, err = db.Exec(`
        CREATE TABLE test_table (
            id INT PRIMARY KEY,
            int_col INT,
            varchar_col VARCHAR(255),
            float_col FLOAT,
            bool_col BIT,
            de DECIMAL(18,6),
            date_col DATE,
            datetime_col DATETIME2,
            timestamp_col DATETIME2
        )
    `)
	if err != nil {
		log.Fatal(err)
	}

	// insert 10 rows
	for i := 1; i <= 10; i++ {
		insert := fmt.Sprintf(`
            INSERT INTO test_table 
            (id, int_col, varchar_col, float_col, de, bool_col, date_col, datetime_col, timestamp_col)
            VALUES 
            (%d, %d, '%s', %f, %d, %d, '%s', '%s', '%s')`,
			i, i, fmt.Sprintf("varchar %d", i), float64(i), i%2, 1,
			"2022-01-01",
			"2022-01-01 00:00:00",
			"2024-06-30 20:00:00")

		_, err = db.Exec(insert)
		if err != nil {
			log.Fatal(err)
		}
	}
	log.Println("===prepareSQLServer done.===")
}

func prepareOracleTestConfig() *cfg.Config {
	config := cfg.Config{
		DatabaseType:         "oracle",
		SourceDB:             "MYDB",
		SourceHost:           "127.0.0.1",
		SourcePort:           49161,
		SourceUser:           "mydb",
		SourcePass:           "mydb",
		SourceTable:          "TEST_TABLE",
		SourceWhereCondition: "id > 0",
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
		OracleSID:            "XE",
	}

	return &config
}

func prepareSqlServerTimeKeyTestConfig() *cfg.Config {
	config := cfg.Config{
		DatabaseType:         "mssql",
		SourceDB:             "mydb",
		SourceHost:           "127.0.0.1",
		SourcePort:           1433,
		SourceUser:           "sa",
		SourcePass:           "Password1234!",
		SourceTable:          "test_table",
		SourceWhereCondition: "timestamp_col > '2024-06-29 00:00:00' and timestamp_col < '2024-07-10 20:00:00'",
		SourceSplitKey:       "",
		SourceSplitTimeKey:   "timestamp_col",
		TimeSplitUnit:        "day",
		DatabendDSN:          "http://databend:databend@localhost:8000",
		DatabendTable:        "default.test_table",
		BatchSize:            5,
		BatchMaxInterval:     3,
		MaxThread:            1,
		CopyForce:            false,
		CopyPurge:            false,
		DeleteAfterSync:      false,
		DisableVariantCheck:  false,
		UserStage:            "~",
	}

	return &config
}

func prepareSqlServerTestConfig() *cfg.Config {
	config := cfg.Config{
		DatabaseType:         "mssql",
		SourceDB:             "mydb",
		SourceHost:           "127.0.0.1",
		SourcePort:           1433,
		SourceUser:           "sa",
		SourcePass:           "Password1234!",
		SourceTable:          "test_table",
		SourceWhereCondition: "id > 0",
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

func prepareMySQLMultipleConfig() *cfg.Config {
	config := cfg.Config{
		SourceDB:             "mydb",
		SourceHost:           "127.0.0.1",
		SourcePort:           3306,
		SourceUser:           "root",
		SourcePass:           "123456",
		SourceDbTables:       []string{"db.*@test_table.*"},
		SourceTable:          "test_table",
		SourceWhereCondition: "id > 0",
		SourceQuery:          "select * from mydb2.test_table",
		SourceSplitKey:       "id",
		SourceSplitTimeKey:   "",
		DatabendDSN:          "http://databend:databend@localhost:8000",
		DatabendTable:        "default.test_table2",
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

func prepareOracleMultipleConfig() *cfg.Config {
	config := cfg.Config{
		DatabaseType:         "oracle",
		SourceHost:           "127.0.0.1",
		SourcePort:           49161,
		SourceUser:           "a",
		SourcePass:           "123",
		SourceDbTables:       []string{"MYDB.*@TEST_TABLE.*"},
		SourceWhereCondition: "id > 0",
		SourceQuery:          "select * from mydb2.test_table",
		SourceSplitKey:       "id",
		SourceSplitTimeKey:   "",
		DatabendDSN:          "http://databend:databend@localhost:8000",
		DatabendTable:        "default.test_table2",
		BatchSize:            5,
		BatchMaxInterval:     3,
		MaxThread:            2,
		CopyForce:            false,
		CopyPurge:            false,
		DeleteAfterSync:      false,
		DisableVariantCheck:  false,
		UserStage:            "~",
		OracleSID:            "XE",
	}

	return &config
}

func checkTargetTable(tableName string, target int) error {
	db, err := sql.Open("databend", "http://databend:databend@localhost:8000")
	if err != nil {
		log.Fatal(err)
		return err
	}
	defer db.Close()

	rows, err := db.Query(fmt.Sprintf(`SELECT * FROM default.%s`, tableName))
	if err != nil {
		log.Fatal(err)
		return err
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
	if count != target {
		return fmt.Errorf("target table count not equal %d", target)
	}
	return nil
}
