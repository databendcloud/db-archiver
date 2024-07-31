package models

import (
	"time"

	"gorm.io/gorm"

	terr "github.com/databendcloud/db-archiver/pkg/errors"
)

type SyncTask struct {
	SyncTaskID           int64  `json:"syncTaskID" gorm:"column:sync_task_id;type:bigint;primary_key"`
	SourceHost           string `json:"sourceHost" gorm:"column:source_host;type:varchar(255)"`
	SourcePort           int    `json:"sourcePort" gorm:"column:source_port;type:int"`
	SourceUser           string `json:"sourceUser" gorm:"column:source_user;type:varchar(255)"`
	SourcePass           string `json:"sourcePass" gorm:"column:source_pass;type:varchar(255)"`
	SourceDB             string `json:"sourceDB" gorm:"column:source_db;type:varchar(255)"`
	SourceTable          string `json:"sourceTable" gorm:"column:source_table;type:varchar(255)"`
	SourceQuery          string `json:"sourceQuery" gorm:"column:source_query;type:varchar(255)"`                    // select * from table where condition
	SourceWhereCondition string `json:"sourceWhereCondition" gorm:"column:source_where_condition;type:varchar(255)"` //example: where id > 100 and id < 200 and time > '2023-01-01'
	SourceSplitKey       string `json:"sourceSplitKey" gorm:"column:source_split_key;type:varchar(255)"`             // primary split key for split table, only for int type
	// the format of time field must be: 2006-01-02 15:04:05
	SourceSplitTimeKey string `json:"SourceSplitTimeKey" gorm:"column:source_split_time_key;type:varchar(255)"`     // time field for split table
	TimeSplitUnit      string `json:"TimeSplitUnit" default:"hour" gorm:"column:time_split_unit;type:varchar(255)"` // time split unit, default is hour, option is: minute, hour, day

	// Databend configuration
	DatabendDSN      string `json:"databendDSN" default:"localhost:8000" gorm:"column:databend_dsn;type:varchar(255)"`
	DatabendDatabase string `json:"databendDatabase" gorm:"column:databend_database;type:varchar(255)"`
	DatabendTable    string `json:"databendTable" gorm:"column:databend_table;type:varchar(255)"`
	BatchSize        int    `json:"batchSize" default:"1000" gorm:"column:batch_size;type:int"`
	//BatchMaxInterval int    `json:"batchMaxInterval" default:"3"` // for rate limit control

	// related docs: https://docs.databend.com/sql/sql-commands/dml/dml-copy-into-table
	CopyPurge       bool      `json:"copyPurge" default:"false" gorm:"column:copy_purge;type:bool"`
	CopyForce       bool      `json:"copyForce" default:"false" gorm:"column:copy_force;type:bool"`
	UserStage       string    `json:"userStage" default:"~" gorm:"column:user_stage;type:varchar(255)"`
	DeleteAfterSync bool      `json:"deleteAfterSync" default:"false" gorm:"column:delete_after_sync;type:bool"`
	MaxThread       int       `json:"maxThread" default:"1" gorm:"column:max_thread;type:int"`
	CreatedAt       time.Time `json:"createdAt" gorm:"column:created_at;type:timestamp with time zone;default:now()"`
	UpdatedAt       time.Time `json:"updatedAt" gorm:"column:updated_at;type:timestamp with time zone;default:now()"`
	DeletedAt       time.Time `json:"deletedAt" gorm:"column:deleted_at;type:timestamp with time zone;default:now()"`
	ShouldRunning   bool      `json:"shouldRunning" default:"true" gorm:"column:should_running;type:bool"`
	SyncRate        string    `json:"syncRate" gorm:"column:sync_rate;type:varchar(255)"`                // 100 rows/s
	SyncPercentage  int       `json:"syncPercentage" default:"0" gorm:"column:sync_percentage;type:int"` // 0-100
	Status          string    `json:"status" gorm:"column:status;type:varchar(255)"`
}

func (SyncTask) TableName() string {
	return "sync_config"
}

func (s *SyncTask) DeepCopy() *SyncTask {
	st := *s
	return &st
}

func (dao *DAO) CreateSync(syncConfig *SyncTask) error {
	return dao.db.Create(syncConfig).Error
}

func (dao *DAO) GetTask(syncTaskId int64) (*SyncTask, error) {
	t := &SyncTask{}
	if err := dao.db.Where(&SyncTask{SyncTaskID: syncTaskId}).First(&t).Error; err != nil {
		switch err {
		case gorm.ErrRecordNotFound:
			return nil, terr.Wrapf(&terr.TaskNotFound, "syncTaskId: %d", syncTaskId)
		default:
			return nil, err
		}
	}
	return t, nil
}

func (dao *DAO) UpdateTaskStatus(syncTaskId int64, status string) error {
	t, err := dao.GetTask(syncTaskId)
	if err != nil {
		return err
	}
	t.Status = status
	t.UpdatedAt = time.Now()
	if err := dao.db.Model(&SyncTask{}).Where("sync_task_id = ?", t.SyncTaskID).Select("status").Updates(t).Error; err != nil {
		return err
	}
	return nil
}

func (dao *DAO) UpdateTaskRate(syncTaskId int64, rate string, syncPercentage int) error {
	t, err := dao.GetTask(syncTaskId)
	if err != nil {
		return err
	}
	t.SyncRate = rate
	t.SyncPercentage = syncPercentage
	t.UpdatedAt = time.Now()
	if err := dao.db.Model(&SyncTask{}).Where("sync_task_id = ?", t.SyncTaskID).Select("sync_rate, sync_percentage").Updates(t).Error; err != nil {
		return err
	}
	return nil
}

func (dao *DAO) ListSyncTasks() ([]SyncTask, error) {
	var tasks []SyncTask
	if err := dao.db.Find(&tasks).Error; err != nil {
		return nil, err
	}
	return tasks, nil
}

func (dao *DAO) DeleteSync(syncTaskID int64) error {
	t, err := dao.GetTask(syncTaskID)
	if err != nil {
		return nil
	}
	// can't delete a running task
	if t.ShouldRunning {
		return terr.Wrapf(&terr.TaskRunning, "syncTaskId: %d", syncTaskID)
	}
	return dao.db.Where("sync_task_id = ?", syncTaskID).Delete(&SyncTask{}).Error
}

func (dao *DAO) StartStopTask(syncTaskID int64, start bool) error {
	t, err := dao.GetTask(syncTaskID)
	if err != nil {
		return err
	}
	t.ShouldRunning = start
	t.UpdatedAt = time.Now()
	if err := dao.db.Model(&SyncTask{}).Where("sync_task_id = ?", t.SyncTaskID).Select("should_running").Updates(t).Error; err != nil {
		return err
	}
	return nil
}
