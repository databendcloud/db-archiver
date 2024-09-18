package source

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/test-go/testify/assert"

	"github.com/databendcloud/db-archiver/config"
	"github.com/databendcloud/db-archiver/utils/testutils"
)

type postgresSourceTest struct {
	postgresSource PostgresSource
}

var postgresPort = 15432

func setupPostgresSourceTest() (*postgresSourceTest, func()) {
	pgDsn, tearDownFunc := testutils.PostgresForTest()
	sourceDbTables := []string{"mydb.*@test_table.*"}
	db, err := sql.Open("postgres", pgDsn)
	if err != nil {
		panic(err)
	}
	_, err = db.Exec(`
DO $$ 
BEGIN 
    IF NOT EXISTS (
        SELECT FROM pg_database WHERE datname = 'mydb'
    ) THEN 
        CREATE DATABASE mydb; 
    END IF; 
END
$$;`)
	if err != nil {
		panic(err)
	}

	db, err = sql.Open("postgres", fmt.Sprintf("postgres://postgres:postgres@localhost:%d/mydb?sslmode=disable&client_encoding=UTF8", postgresPort))

	_, err = db.Exec("CREATE TABLE  test_table (id SERIAL primary key, name varchar(255), ts timestamp default current_timestamp)")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("INSERT INTO test_table (name) VALUES ('test')")
	_, err = db.Exec("INSERT INTO test_table (name) VALUES ('test2')")
	if err != nil {
		panic(err)
	}
	cfg := &config.Config{
		DatabendDSN:          "http://databend:databend@localhost:8080",
		DatabendTable:        "default.test_table",
		DatabaseType:         "postgres",
		SourceHost:           "localhost",
		SourcePort:           postgresPort,
		SourceUser:           "postgres",
		SourcePass:           "postgres",
		SourceDbTables:       sourceDbTables,
		SourceDB:             "mydb",
		SourceWhereCondition: "id > 0",
		SSLMode:              "disable",
		SourceTable:          "test_table",
		SourceSplitKey:       "id",
		SourceSplitTimeKey:   "ts",
		BatchSize:            1000,
		BatchMaxInterval:     3,
		MaxThread:            2,
		CopyPurge:            true,
	}
	source, err := NewPostgresSource(cfg)
	if err != nil {
		panic(err)
	}
	return &postgresSourceTest{
		postgresSource: *source,
	}, tearDownFunc
}

func TestPostgresSource_GetDbTablesAccordingToSourceDbTables(t *testing.T) {
	postgresSourceTest, tearDownFunc := setupPostgresSourceTest()
	defer tearDownFunc()
	tables, err := postgresSourceTest.postgresSource.GetDbTablesAccordingToSourceDbTables()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tables))
	assert.Equal(t, "test_table", tables["mydb"])
}

func TestPostgresSource_GetTablesAccordingToSourceTableRegex(t *testing.T) {
	postgresSourceTest, tearDownFunc := setupPostgresSourceTest()
	defer tearDownFunc()
	tables, err := postgresSourceTest.postgresSource.GetTablesAccordingToSourceTableRegex("test_table", []string{"mydb"})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tables))
	assert.Equal(t, "test_table", tables["mydb"][0])
}

func TestPostgresSource_GetDatabasesAccordingToSourceDbRegex(t *testing.T) {
	postgresSourceTest, tearDownFunc := setupPostgresSourceTest()
	defer tearDownFunc()
	dbs, err := postgresSourceTest.postgresSource.GetDatabasesAccordingToSourceDbRegex("mydb")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(dbs))
	assert.Equal(t, "mydb", dbs[0])
}

func TestPostgresSource_GetSourceReadRowsCount(t *testing.T) {
	postgresSourceTest, tearDownFunc := setupPostgresSourceTest()
	defer tearDownFunc()
	count, err := postgresSourceTest.postgresSource.GetSourceReadRowsCount()
	assert.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestPostgresSource_GetAllSourceReadRowsCount(t *testing.T) {
	postgresSourceTest, tearDownFunc := setupPostgresSourceTest()
	defer tearDownFunc()
	count, err := postgresSourceTest.postgresSource.GetAllSourceReadRowsCount()
	assert.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestPostgresSource_QueryTableData(t *testing.T) {
	postgresSourceTest, tearDownFunc := setupPostgresSourceTest()
	defer tearDownFunc()
	data, columns, err := postgresSourceTest.postgresSource.QueryTableData(1, "id > 0")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(data))
	assert.Equal(t, 2, len(columns))
	assert.Equal(t, "id", columns[0])
	assert.Equal(t, "name", columns[1])
	t.Log(data)
	assert.Equal(t, int64(1), data[0][0])
}

func TestPostgresSource_GetMinMaxSplitKey(t *testing.T) {
	postgresSourceTest, tearDownFunc := setupPostgresSourceTest()
	defer tearDownFunc()
	min, max, err := postgresSourceTest.postgresSource.GetMinMaxSplitKey()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), min)
	assert.Equal(t, int64(2), max)
}

func TestPostgresSource_GetMinMaxTimeSplitKey(t *testing.T) {
	postgresSourceTest, tearDownFunc := setupPostgresSourceTest()
	defer tearDownFunc()
	min, max, err := postgresSourceTest.postgresSource.GetMinMaxTimeSplitKey()
	assert.NoError(t, err)
	assert.NotEmpty(t, min)
	assert.NotEmpty(t, max)
}

func TestPostgresSource_DeleteAfterSync(t *testing.T) {
	postgresSourceTest, tearDownFunc := setupPostgresSourceTest()
	defer tearDownFunc()
	postgresSourceTest.postgresSource.cfg.DeleteAfterSync = true
	err := postgresSourceTest.postgresSource.DeleteAfterSync()
	assert.NoError(t, err)
}

func TestPostgresSource_SwitchDatabase(t *testing.T) {
	postgresSourceTest, tearDownFunc := setupPostgresSourceTest()
	defer tearDownFunc()
	err := postgresSourceTest.postgresSource.SwitchDatabase()
	assert.NoError(t, err)
}

func TestPostgresSource_AdjustBatchSizeAccordingToSourceDbTable(t *testing.T) {
	postgresSourceTest, tearDownFunc := setupPostgresSourceTest()
	defer tearDownFunc()
	batchSize := postgresSourceTest.postgresSource.AdjustBatchSizeAccordingToSourceDbTable()
	assert.Equal(t, int64(2), batchSize)
}
