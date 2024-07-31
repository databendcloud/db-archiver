package sync_logic

import (
	"github.com/databendcloud/db-archiver/config"
	"github.com/databendcloud/db-archiver/pkg/models"
)

type SyncLogicImpl struct {
	dao *models.DAO
	cfg *config.Config
}

type SyncLogic interface {
}