package storage_test

import (
	"database/sql"
	"os"
	"testing"

	"github.com/syncromatics/go-kit/database"
	"github.com/syncromatics/go-kit/testing/docker"

	_ "github.com/syncromatics/kafmesh/internal/storage/statik"
)

var (
	databaseSettings *database.PostgresDatabaseSettings
	db               *sql.DB
)

func TestMain(m *testing.M) {
	var err error
	db, databaseSettings, err = docker.SetupPostgresDatabase("storage")
	if err != nil {
		panic(err)
	}

	err = databaseSettings.MigrateUpWithStatik("/")
	if err != nil {
		panic(err)
	}

	result := m.Run()

	docker.TeardownPostgresDatabase("storage")

	os.Exit(result)
}
