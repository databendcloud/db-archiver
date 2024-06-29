package source

import (
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
