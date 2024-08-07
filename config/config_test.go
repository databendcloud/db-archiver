package config

import (
	"testing"
	"time"
)

func TestValidateSourceSplitTimeKey(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr bool
	}{
		{
			name:    "valid format 1",
			value:   "t1 > '2024-06-30 2:00:00' and t1 < '2024-06-30 20:00:00'",
			wantErr: false,
		},
		{
			name:    "valid format 1",
			value:   "t1>'2024-06-30 2:00:00' and t1< '2024-06-30 20:00:00'",
			wantErr: false,
		},

		{
			name:    "valid format 2",
			value:   "field >= 'x' and field <= 'y'",
			wantErr: false,
		},
		{
			name:    "valid format 3",
			value:   "field >= 'x' and field < 'y'",
			wantErr: false,
		},
		{
			name:    "valid format 4",
			value:   "field > 'x' and field <= 'y'",
			wantErr: false,
		},
		{
			name:    "invalid format",
			value:   "field > 'x' and field 'y'",
			wantErr: true,
		},
		{
			name:    "invalid format",
			value:   "field > 'x'",
			wantErr: true,
		},
		{
			name:    "invalid format",
			value:   "field >= 'x'",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSourceSplitTimeKey(tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateSourceSplitTimeKey() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestGetTimeRangeBySplitUnit(t *testing.T) {
	tests := []struct {
		name     string
		unit     string
		expected time.Duration
	}{
		{
			name:     "Minute unit returns 10 minutes",
			unit:     "minute",
			expected: 10 * time.Minute,
		},
		{
			name:     "Quarter unit returns 15 minutes",
			unit:     "quarter",
			expected: 15 * time.Minute,
		},
		{
			name:     "Hour unit returns 2 hours",
			unit:     "hour",
			expected: 2 * time.Hour,
		},
		{
			name:     "Day unit returns 24 hours",
			unit:     "day",
			expected: 24 * time.Hour,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{TimeSplitUnit: tt.unit}
			got := cfg.GetTimeRangeBySplitUnit()
			if got != tt.expected {
				t.Errorf("GetTimeRangeBySplitUnit() = %v, want %v", got, tt.expected)
			}
		})
	}
}
