package testutils

import (
	"fmt"
	"os"
	"sync"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
)

var (
	testPostgres     *embeddedpostgres.EmbeddedPostgres
	testPostgresOnce sync.Once
)

const testPostgresPort = 15432

func PostgresForTest() (string, func()) {
	// take an external postgres DSN
	if dsn := os.Getenv("TEST_POSTGRES_DSN"); dsn != "" {
		return dsn, func() {}
	}

	// take a long lived postgres instance TEST_POSTGRES_DSN is set
	dsn := fmt.Sprintf("postgres://postgres:postgres@localhost:%d/mydb?sslmode=disable&client_encoding=UTF8", testPostgresPort)
	if os.Getenv("TEST_POSTGRES_SINGLETON") != "" {
		testPostgresOnce.Do(func() {
			testPostgres = embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().Database("mydb").
				Port(testPostgresPort))
			if err := testPostgres.Start(); err != nil {
				panic(err)
			}
		})
		return dsn, func() {}
	}

	// take a temp postgres instance
	fakePostgresDB := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().Database("mydb").Port(testPostgresPort))
	if err := fakePostgresDB.Start(); err != nil {
		panic(err)
	}
	return dsn, func() {
		err := fakePostgresDB.Stop()
		if err != nil {
			panic(err)
		}
	}
}
