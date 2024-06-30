package source

import (
	"fmt"
	"testing"

	"github.com/databendcloud/db-archiver/config"
)

func NewMockSource(cfg *config.Config) (*Source, error) {
	return &Source{
		cfg: cfg,
	}, nil
}

func TestSlimCondition(t *testing.T) {
	cfg := &config.Config{
		SourceSplitKey: "id",
		MaxThread:      5,
	}
	source, _ := NewMockSource(cfg)

	// Test when minSplitKey is less than maxSplitKey
	conditions := source.SlimCondition(0, 100)
	if len(conditions) != 5 {
		t.Errorf("Expected 5 conditions, got %d", len(conditions))
	}
	if conditions[4][1] != 100 {
		t.Errorf("Expected last upperBound to be 100, got %d", conditions[4][1])
	}

	// Test when minSplitKey is greater than maxSplitKey
	conditions = source.SlimCondition(200, 100)
	if len(conditions) != 0 {
		t.Errorf("Expected 0 conditions, got %d", len(conditions))
	}
}

func TestSlimConditionWithMaxThreadOne(t *testing.T) {
	cfg := &config.Config{
		SourceSplitKey: "id",
		MaxThread:      1,
	}
	source, _ := NewMockSource(cfg)

	// Test when minSplitKey is less than maxSplitKey
	conditions := source.SlimCondition(0, 100)
	if len(conditions) != 1 {
		t.Errorf("Expected 1 condition, got %d", len(conditions))
	}
	if conditions[0][1] != 100 {
		t.Errorf("Expected last upperBound to be 100, got %d", conditions[0][1])
	}

	// Test when minSplitKey is equal to maxSplitKey
	conditions = source.SlimCondition(100, 100)
	if len(conditions) != 1 {
		t.Errorf("Expected 1 condition, got %d", len(conditions))
	}
	if conditions[0][1] != 100 {
		t.Errorf("Expected last upperBound to be 100, got %d", conditions[0][1])
	}

	// Test when minSplitKey is greater than maxSplitKey
	conditions = source.SlimCondition(200, 100)
	if len(conditions) != 0 {
		t.Errorf("Expected 0 conditions, got %d", len(conditions))
	}
}

func TestSplitConditionAccordingMaxGoRoutine(t *testing.T) {
	cfg := &config.Config{
		SourceSplitKey: "id",
		MaxThread:      5,
		BatchSize:      10,
	}
	source, _ := NewMockSource(cfg)

	// Test when minSplitKey is less than maxSplitKey and maxSplitKey is less than allMax
	conditions := source.SplitConditionAccordingMaxGoRoutine(0, 50, 100)
	if len(conditions) != 7 {
		t.Errorf("Expected 7 conditions, got %d", len(conditions))
	}
	if conditions[4] != fmt.Sprintf("(%s >= %d and %s < %d)", cfg.SourceSplitKey, 36, cfg.SourceSplitKey, 45) {
		t.Errorf("Expected last condition to be (%s >= %d and %s < %d), got %s", cfg.SourceSplitKey, 36, cfg.SourceSplitKey, 45, conditions[4])
	}

	// Test when minSplitKey is less than maxSplitKey and maxSplitKey is equal to allMax
	conditions = source.SplitConditionAccordingMaxGoRoutine(0, 100, 100)
	println(conditions)
	if len(conditions) != 12 {
		t.Errorf("Expected 12 conditions, got %d", len(conditions))
	}
	if conditions[9] != fmt.Sprintf("(%s >= %d and %s < %d)", cfg.SourceSplitKey, 81, cfg.SourceSplitKey, 90) {
		t.Errorf("Expected last condition to be (%s >= %d and %s < %d), got %s", cfg.SourceSplitKey, 81, cfg.SourceSplitKey, 90, conditions[9])
	}

	// Test when minSplitKey is greater than maxSplitKey
	conditions = source.SplitConditionAccordingMaxGoRoutine(200, 100, 300)
	if len(conditions) != 0 {
		t.Errorf("Expected 0 conditions, got %d", len(conditions))
	}
}

func TestSplitConditionAccordingToTimeSplitKey(t *testing.T) {
	cfg := &config.Config{
		SourceSplitTimeKey: "t1",
	}
	source, _ := NewMockSource(cfg)

	// Test when minTimeSplitKey is less than maxTimeSplitKey
	conditions, err := source.SplitConditionAccordingToTimeSplitKey("2024-06-30 2:00:00", "2024-06-30 20:00:00")
	fmt.Println(conditions)
	if err != nil {
		t.Errorf("SplitConditionAccordingToTimeSplitKey() error = %v", err)
	}
	if len(conditions) != 108 {
		t.Errorf("Expected 108 conditions, got %d", len(conditions))
	}

	// Test when minTimeSplitKey is equal to maxTimeSplitKey
	conditions, err = source.SplitConditionAccordingToTimeSplitKey("2024-06-30 2:00:00", "2024-06-30 2:00:00")
	if err != nil {
		t.Errorf("SplitConditionAccordingToTimeSplitKey() error = %v", err)
	}
	if len(conditions) != 0 {
		t.Errorf("Expected 0 conditions, got %d", len(conditions))
	}

	// Test when minTimeSplitKey is greater than maxTimeSplitKey
	conditions, err = source.SplitConditionAccordingToTimeSplitKey("2024-06-30 20:00:00", "2024-06-30 2:00:00")
	if err != nil {
		t.Errorf("SplitConditionAccordingToTimeSplitKey() error = %v", err)
	}
	if len(conditions) != 0 {
		t.Errorf("Expected 0 conditions, got %d", len(conditions))
	}
}
