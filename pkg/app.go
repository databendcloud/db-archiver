package pkg

import (
	"github.com/databendcloud/db-archiver/config"
	"github.com/databendcloud/db-archiver/pkg/models"
)

type App struct {
	cfg *config.Config
	dao *models.DAO
}

func NewApp(conf *config.Config) (*App, error) {
	var err error
	app := &App{cfg: conf}
	app.dao = models.NewDao()

	return app, err
}
