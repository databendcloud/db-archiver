package ingester

import (
	"sync"
	"time"

	timeseries "github.com/codesuki/go-time-series"
)

type DatabendIngesterStatsRecorder struct {
	ingestedBytes *timeseries.TimeSeries
	ingestedRows  *timeseries.TimeSeries
	mu            sync.Mutex
}

type DatabendIngesterStatsData struct {
	BytesPerSecond float64
	RowsPerSecondd float64
}

func NewDatabendIntesterStatsRecorder() *DatabendIngesterStatsRecorder {
	ingestedBytes, err := timeseries.NewTimeSeries()
	if err != nil {
		panic(err)
	}
	ingestedRows, err := timeseries.NewTimeSeries()
	if err != nil {
		panic(err)
	}
	return &DatabendIngesterStatsRecorder{
		ingestedBytes: ingestedBytes,
		ingestedRows:  ingestedRows,
	}
}

func (stats *DatabendIngesterStatsRecorder) RecordMetric(bytes int, rows int) {
	stats.mu.Lock()
	defer stats.mu.Unlock()
	stats.ingestedBytes.Increase(bytes)
	stats.ingestedRows.Increase(rows)
}

func (stats *DatabendIngesterStatsRecorder) Stats(statsWindow time.Duration) DatabendIngesterStatsData {
	stats.mu.Lock()
	defer stats.mu.Unlock()

	bytesPerSecond := stats.calcPerSecond(stats.ingestedBytes, statsWindow)
	rowsPerSecond := stats.calcPerSecond(stats.ingestedRows, statsWindow)
	return DatabendIngesterStatsData{
		BytesPerSecond: bytesPerSecond,
		RowsPerSecondd: rowsPerSecond,
	}
}

func (stats *DatabendIngesterStatsRecorder) calcPerSecond(ts *timeseries.TimeSeries, duration time.Duration) float64 {
	amount, err := ts.Range(time.Now().Add(-duration), time.Now())
	if err != nil {
		return -1
	}

	return float64(amount) / duration.Seconds()
}
