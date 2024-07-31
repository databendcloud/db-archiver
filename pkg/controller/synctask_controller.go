package controller

import (
	"github.com/gin-gonic/gin"

	"github.com/databendcloud/db-archiver/pkg/logic/sync_logic"
)

type SyncTaskController struct {
	l *sync_logic.SyncLogic
}

func NewSyncTaskController(l *sync_logic.SyncLogic) *SyncTaskController {
	return &SyncTaskController{l: l}
}

func (ctl *SyncTaskController) InjectRouters(r *gin.Engine) {
	//g := r.Group("/api/v1")
	//g.POST()
}
