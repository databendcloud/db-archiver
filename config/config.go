package config

import (
	"encoding/json"
	"fmt"
	"os"
)

type Config struct {
	// Source configuration
	SourceHost           string `json:"sourceHost"`
	SourcePort           int    `json:"sourcePort"`
	SourceUser           string `json:"sourceUser"`
	SourcePass           string `json:"sourcePass"`
	SourceDB             string `json:"sourceDB"`
	SourceTable          string `json:"sourceTable"`
	SourceQuery          string `json:"sourceQuery"`          // select * from table where condition
	SourceWhereCondition string `json:"sourceWhereCondition"` //example: where id > 100 and id < 200 and time > '2023-01-01'
	SourceSplitKey       string `json:"sourceSplitKey"`       // primary split key for split table, only for int type

	// Databend configuration
	DatabendDSN      string `json:"databendDSN" default:"localhost:8000"`
	DatabendTable    string `json:"databendTable"`
	BatchSize        int    `json:"batchSize" default:"1000"`
	BatchMaxInterval int    `json:"batchMaxInterval" default:"30"`
	Workers          int    `json:"workers" default:"1"`

	// related docs: https://docs.databend.com/sql/sql-commands/dml/dml-copy-into-table
	CopyPurge           bool   `json:"copyPurge" default:"false"`
	CopyForce           bool   `json:"copyForce" default:"false"`
	DisableVariantCheck bool   `json:"disableVariantCheck" default:"false"`
	UserStage           string `json:"userStage" default:"~"`
	DeleteAfterSync     bool   `json:"deleteAfterSync" default:"false"`

	// 多表并行归档
	// 生成配置文件，
	// 限流的情况
}

func LoadConfig() (*Config, error) {
	conf := Config{}

	f, err := os.Open("config/conf.json")
	if err != nil {
		return nil, err
	}
	defer f.Close()
	decoder := json.NewDecoder(f)
	err = decoder.Decode(&conf)
	if err != nil {
		fmt.Println("Error decoding JSON:", err)
		return &conf, err
	}

	return &conf, nil
}
