package source

import (
	"sync"
	"time"

	timeseries "github.com/codesuki/go-time-series"
)

type DatabendSourceStatsRecorder struct {
	extractRows *timeseries.TimeSeries
	mu          sync.Mutex
}

type DatabendIngesterStatsData struct {
	BytesPerSecond float64
	RowsPerSecondd float64
}

func NewDatabendIntesterStatsRecorder() *DatabendSourceStatsRecorder {
	ingestedRows, err := timeseries.NewTimeSeries()
	if err != nil {
		panic(err)
	}
	return &DatabendSourceStatsRecorder{
		extractRows: ingestedRows,
	}
}

func (stats *DatabendSourceStatsRecorder) RecordMetric(rows int) {
	stats.mu.Lock()
	defer stats.mu.Unlock()
	stats.extractRows.Increase(rows)
}

func (stats *DatabendSourceStatsRecorder) Stats(statsWindow time.Duration) DatabendIngesterStatsData {
	stats.mu.Lock()
	defer stats.mu.Unlock()

	rowsPerSecond := stats.calcPerSecond(stats.extractRows, statsWindow)
	return DatabendIngesterStatsData{
		RowsPerSecondd: rowsPerSecond,
	}
}

func (stats *DatabendSourceStatsRecorder) calcPerSecond(ts *timeseries.TimeSeries, duration time.Duration) float64 {
	amount, err := ts.Range(time.Now().Add(-duration), time.Now())
	if err != nil {
		return -1
	}

	return float64(amount) / duration.Seconds()
}
