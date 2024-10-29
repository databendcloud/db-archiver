package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"
)

type Config struct {
	DatabaseType         string   `json:"databaseType"`
	SourceHost           string   `json:"sourceHost"`
	SourcePort           int      `json:"sourcePort"`
	SourceUser           string   `json:"sourceUser"`
	SourcePass           string   `json:"sourcePass"`
	SourceDB             string   `json:"sourceDB"`
	SSLMode              string   `json:"sslMode"`
	SourceTable          string   `json:"sourceTable"`
	SourceDbTables       []string `json:"sourceDbTables"`
	SourceQuery          string   `json:"sourceQuery"`
	SourceWhereCondition string   `json:"sourceWhereCondition"`
	SourceSplitKey       string   `json:"sourceSplitKey"`
	SourceSplitTimeKey   string   `json:"sourceSplitTimeKey"`
	TimeSplitUnit        string   `json:"timeSplitUnit"`
	DatabendDSN          string   `json:"databendDSN"`
	DatabendTable        string   `json:"databendTable"`
	BatchSize            int64    `json:"batchSize"`
	BatchMaxInterval     int      `json:"batchMaxInterval"`
	CopyPurge            bool     `json:"copyPurge"`
	CopyForce            bool     `json:"copyForce"`
	DisableVariantCheck  bool     `json:"disableVariantCheck"`
	UserStage            string   `json:"userStage"`
	DeleteAfterSync      bool     `json:"deleteAfterSync"`
	MaxThread            int      `json:"maxThread"`
}

func main() {
	templatePath := flag.String("template", "conf.json", "Path to template conf.json")
	sourceDB := flag.String("sourceDb", "", "Source database name")
	sourceTable := flag.String("sourceTable", "", "Source table name")
	targetDBTable := flag.String("targetDbTable", "", "Target database and table name")
	timeUnit := flag.String("timeunit", "day", "Time unit (day/week/month)")
	flag.Parse()

	if *sourceDB == "" || *sourceTable == "" {
		fmt.Println("Please provide source database and table names")
		flag.Usage()
		os.Exit(1)
	}

	templateData, err := os.ReadFile(*templatePath)
	if err != nil {
		fmt.Printf("Error reading template file: %v\n", err)
		os.Exit(1)
	}

	var config Config
	if err := json.Unmarshal(templateData, &config); err != nil {
		fmt.Printf("Error parsing template JSON: %v\n", err)
		os.Exit(1)
	}

	// cal time range
	now := time.Now()
	var startTime time.Time

	switch *timeUnit {
	case "day":
		startTime = now.AddDate(0, 0, -1)
	case "week":
		startTime = now.AddDate(0, 0, -7)
	case "month":
		startTime = now.AddDate(0, -1, 0)
	default:
		fmt.Println("Invalid time unit. Must be day, week, or month")
		os.Exit(1)
	}

	// update conf
	config.SourceDB = *sourceDB
	config.SourceTable = *sourceTable
	config.SourceQuery = fmt.Sprintf("select * from %s.%s", *sourceDB, *sourceTable)
	config.SourceWhereCondition = fmt.Sprintf("t1 >= '%s' AND t1 < '%s'",
		startTime.Format("2006-01-02 15:04:05"),
		now.Format("2006-01-02 15:04:05"))
	config.DatabendTable = *targetDBTable

	// 使用自定义编码器，禁用 HTML 转义
	buf := new(bytes.Buffer)
	encoder := json.NewEncoder(buf)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "    ")

	if err := encoder.Encode(config); err != nil {
		fmt.Printf("Error marshaling JSON: %v\n", err)
		os.Exit(1)
	}

	outputPath := "conf.json"
	if err := os.WriteFile(outputPath, buf.Bytes(), 0644); err != nil {
		fmt.Printf("Error writing file: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Configuration file generated successfully: %s\n", outputPath)
}
