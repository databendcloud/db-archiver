package config

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"

	"github.com/pkg/errors"
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
	SourceSplitTimeKey   string `json:"SourceSplitTimeKey"`   // time field for split table

	// Databend configuration
	DatabendDSN      string `json:"databendDSN" default:"localhost:8000"`
	DatabendTable    string `json:"databendTable"`
	BatchSize        int    `json:"batchSize" default:"1000"`
	BatchMaxInterval int    `json:"batchMaxInterval" default:"3"` // for rate limit control

	// related docs: https://docs.databend.com/sql/sql-commands/dml/dml-copy-into-table
	CopyPurge           bool   `json:"copyPurge" default:"false"`
	CopyForce           bool   `json:"copyForce" default:"false"`
	DisableVariantCheck bool   `json:"disableVariantCheck" default:"false"`
	UserStage           string `json:"userStage" default:"~"`
	DeleteAfterSync     bool   `json:"deleteAfterSync" default:"false"`
	MaxThread           int    `json:"maxThread" default:"2"`
}

func LoadConfig(configFile string) (*Config, error) {
	conf := Config{}

	f, err := os.Open(configFile)
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

func preCheckConfig(cfg *Config) {
	if cfg.SourceSplitKey != "" && cfg.SourceSplitTimeKey != "" {
		panic("cannot set both sourceSplitKey and sourceSplitTimeKey")
	}
	if cfg.SourceSplitKey == "" && cfg.SourceSplitTimeKey == "" {
		panic("must set one of sourceSplitKey and sourceSplitTimeKey")
	}
	if cfg.SourceSplitTimeKey != "" || cfg.SourceSplitKey != "" {
		if cfg.SourceWhereCondition == "" {
			panic("must set sourceWhereCondition when sourceSplitTimeKey is set")
		}
	}
	if cfg.SourceSplitTimeKey != "" {
		// time warehouse condition must be  x < time < y
		err := validateSourceSplitTimeKey(cfg.SourceSplitTimeKey)
		if err != nil {
			panic(err)
		}
	}
}

func validateSourceSplitTimeKey(value string) error {
	// 正则表达式匹配 field>x and field <y 或者 field >= x and field <=y, 或者 field >=x and field <y, 或者 field>x and field <=y 的格式
	pattern := `^\w+\s*(>|>=)\s*x\s+and\s+\w+\s*(<|<=)\s*y$`
	matched, err := regexp.MatchString(pattern, value)
	if err != nil {
		return err
	}
	if !matched {
		return errors.New("SourceSplitTimeKey does not match the required format")
	}
	return nil
}
