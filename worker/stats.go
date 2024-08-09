package worker

import (
	"sync"
	"time"

	timeseries "github.com/codesuki/go-time-series"
)

type DatabendWorkerStatsRecorder struct {
	ingestedBytes *timeseries.TimeSeries
	ingestedRows  *timeseries.TimeSeries
	mu            sync.Mutex
}

type DatabendWorkerStatsData struct {
	BytesPerSecond float64
	RowsPerSecondd float64
}

func NewDatabendWorkerStatsRecorder() *DatabendWorkerStatsRecorder {
	ingestedBytes, err := timeseries.NewTimeSeries()
	if err != nil {
		panic(err)
	}
	ingestedRows, err := timeseries.NewTimeSeries()
	if err != nil {
		panic(err)
	}
	return &DatabendWorkerStatsRecorder{
		ingestedBytes: ingestedBytes,
		ingestedRows:  ingestedRows,
	}
}

func (stats *DatabendWorkerStatsRecorder) RecordMetric(bytes int, rows int) {
	stats.mu.Lock()
	defer stats.mu.Unlock()
	stats.ingestedBytes.Increase(bytes)
	stats.ingestedRows.Increase(rows)
}

func (stats *DatabendWorkerStatsRecorder) Stats(statsWindow time.Duration) DatabendWorkerStatsData {
	stats.mu.Lock()
	defer stats.mu.Unlock()

	bytesPerSecond := stats.calcPerSecond(stats.ingestedBytes, statsWindow)
	rowsPerSecond := stats.calcPerSecond(stats.ingestedRows, statsWindow)
	return DatabendWorkerStatsData{
		BytesPerSecond: bytesPerSecond,
		RowsPerSecondd: rowsPerSecond,
	}
}

func (stats *DatabendWorkerStatsRecorder) calcPerSecond(ts *timeseries.TimeSeries, duration time.Duration) float64 {
	amount, err := ts.Range(time.Now().Add(-duration), time.Now())
	if err != nil {
		return -1
	}

	return float64(amount) / duration.Seconds()
}
