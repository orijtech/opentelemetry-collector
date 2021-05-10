// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"go.opentelemetry.io/collector/consumer/pdata"
)

// Notes on garbage collection (gc):
//
// Job-level gc:
// The Prometheus receiver will likely execute in a long running service whose lifetime may exceed
// the lifetimes of many of the jobs that it is collecting from. In order to keep the JobsMap from
// leaking memory for entries of no-longer existing jobs, the JobsMap needs to remove entries that
// haven't been accessed for a long period of time.
//
// Timeseries-level gc:
// Some jobs that the Prometheus receiver is collecting from may export timeseries based on metrics
// from other jobs (e.g. cAdvisor). In order to keep the timeseriesMap from leaking memory for entries
// of no-longer existing jobs, the timeseriesMap for each job needs to remove entries that haven't
// been accessed for a long period of time.
//
// The gc strategy uses a standard mark-and-sweep approach - each time a timeseriesMap is accessed,
// it is marked. Similarly, each time a timeseriesinfo is accessed, it is also marked.
//
// At the end of each JobsMap.get(), if the last time the JobsMap was gc'd exceeds the 'gcInterval',
// the JobsMap is locked and any timeseriesMaps that are unmarked are removed from the JobsMap
// otherwise the timeseriesMap is gc'd
//
// The gc for the timeseriesMap is straightforward - the map is locked and, for each timeseriesinfo
// in the map, if it has not been marked, it is removed otherwise it is unmarked.
//
// Alternative Strategies
// 1. If the job-level gc doesn't run often enough, or runs too often, a separate go routine can
//    be spawned at JobMap creation time that gc's at periodic intervals. This approach potentially
//    adds more contention and latency to each scrape so the current approach is used. Note that
//    the go routine will need to be cancelled upon Shutdown().
// 2. If the gc of each timeseriesMap during the gc of the JobsMap causes too much contention,
//    the gc of timeseriesMaps can be moved to the end of MetricsAdjuster().AdjustMetrics(). This
//    approach requires adding 'lastGC' Time and (potentially) a gcInterval duration to
//    timeseriesMap so the current approach is used instead.

// timeseriesinfo contains the information necessary to adjust from the initial point and to detect
// resets.
type timeseriesinfo struct {
	mark      bool
	initial   *pdata.Metric
	previous  *pdata.Metric
	histogram *histogram
}

type histogram struct {
	initial  *pdata.HistogramDataPoint
	previous *pdata.HistogramDataPoint
}

// timeseriesMap maps from a timeseries instance (metric * label values) to the timeseries info for
// the instance.
type timeseriesMap struct {
	sync.RWMutex
	mark   bool
	tsiMap map[string]*timeseriesinfo
}

// Get the timeseriesinfo for the timeseries associated with the metric and label values.
func (tsm *timeseriesMap) get(metric *pdata.Metric, values pdata.StringMap) *timeseriesinfo {
	name := metric.Name()
	sig := getTimeseriesSignature(name, values)
	tsi, ok := tsm.tsiMap[sig]
	if !ok {
		tsi = &timeseriesinfo{histogram: new(histogram)}
		tsm.tsiMap[sig] = tsi
	}
	tsm.mark = true
	tsi.mark = true
	return tsi
}

// Remove timeseries that have aged out.
func (tsm *timeseriesMap) gc() {
	tsm.Lock()
	defer tsm.Unlock()
	// this shouldn't happen under the current gc() strategy
	if !tsm.mark {
		return
	}
	for ts, tsi := range tsm.tsiMap {
		if !tsi.mark {
			delete(tsm.tsiMap, ts)
		} else {
			tsi.mark = false
		}
	}
	tsm.mark = false
}

func newTimeseriesMap() *timeseriesMap {
	return &timeseriesMap{mark: true, tsiMap: map[string]*timeseriesinfo{}}
}

// Create a unique timeseries signature consisting of the metric name and label values.
func getTimeseriesSignature(name string, values pdata.StringMap) string {
	labelValues := make([]string, 0, values.Len())
	values.Range(func(key, value string) bool {
		if value != "" {
			labelValues = append(labelValues, value)
		}
		return true
	})
	return fmt.Sprintf("%s,%s", name, strings.Join(labelValues, ","))
}

// JobsMap maps from a job instance to a map of timeseries instances for the job.
type JobsMap struct {
	sync.RWMutex
	gcInterval time.Duration
	lastGC     time.Time
	jobsMap    map[string]*timeseriesMap
}

// NewJobsMap creates a new (empty) JobsMap.
func NewJobsMap(gcInterval time.Duration) *JobsMap {
	return &JobsMap{gcInterval: gcInterval, lastGC: time.Now(), jobsMap: make(map[string]*timeseriesMap)}
}

// Remove jobs and timeseries that have aged out.
func (jm *JobsMap) gc() {
	jm.Lock()
	defer jm.Unlock()
	// once the structure is locked, confirm that gc() is still necessary
	if time.Since(jm.lastGC) > jm.gcInterval {
		for sig, tsm := range jm.jobsMap {
			tsm.RLock()
			tsmNotMarked := !tsm.mark
			tsm.RUnlock()
			if tsmNotMarked {
				delete(jm.jobsMap, sig)
			} else {
				tsm.gc()
			}
		}
		jm.lastGC = time.Now()
	}
}

func (jm *JobsMap) maybeGC() {
	// speculatively check if gc() is necessary, recheck once the structure is locked
	jm.RLock()
	defer jm.RUnlock()
	if time.Since(jm.lastGC) > jm.gcInterval {
		go jm.gc()
	}
}

func (jm *JobsMap) get(job, instance string) *timeseriesMap {
	sig := job + ":" + instance
	jm.RLock()
	tsm, ok := jm.jobsMap[sig]
	jm.RUnlock()
	defer jm.maybeGC()
	if ok {
		return tsm
	}
	jm.Lock()
	defer jm.Unlock()
	tsm2, ok2 := jm.jobsMap[sig]
	if ok2 {
		return tsm2
	}
	tsm2 = newTimeseriesMap()
	jm.jobsMap[sig] = tsm2
	return tsm2
}

// MetricsAdjuster takes a map from a metric instance to the initial point in the metrics instance
// and provides AdjustMetrics, which takes a sequence of metrics and adjust their values based on
// the initial points.
type MetricsAdjuster struct {
	tsm    *timeseriesMap
	logger *zap.Logger
}

// NewMetricsAdjuster is a constructor for MetricsAdjuster.
func NewMetricsAdjuster(tsm *timeseriesMap, logger *zap.Logger) *MetricsAdjuster {
	return &MetricsAdjuster{
		tsm:    tsm,
		logger: logger,
	}
}

// AdjustMetrics takes a sequence of metrics and adjust their values based on the initial and
// previous points in the timeseriesMap. If the metric is the first point in the timeseries, or the
// timeseries has been reset, it is removed from the sequence and added to the timeseriesMap.
// Additionally returns the total number of timeseries dropped from the metrics.
func (ma *MetricsAdjuster) AdjustMetrics(metrics []*pdata.Metric) ([]*pdata.Metric, int) {
	var adjusted = make([]*pdata.Metric, 0, len(metrics))
	dropped := 0
	ma.tsm.Lock()
	defer ma.tsm.Unlock()
	for _, metric := range metrics {
		adj, d := ma.adjustMetric(metric)
		dropped += d
		if adj {
			adjusted = append(adjusted, metric)
		}
	}
	return adjusted, dropped
}

// Returns true if at least one of the metric's timeseries was adjusted and false if all of the
// timeseries are an initial occurrence or a reset. Additionally returns the number of timeseries
// dropped from the metric.
//
// Types of metrics returned supported by prometheus:
// - MetricDescriptor_GAUGE_DOUBLE
// - MetricDescriptor_GAUGE_DISTRIBUTION
// - MetricDescriptor_CUMULATIVE_DOUBLE
// - MetricDescriptor_CUMULATIVE_DISTRIBUTION
// - MetricDescriptor_SUMMARY
func (ma *MetricsAdjuster) adjustMetric(metric *pdata.Metric) (bool, int) {
	switch metric.DataType() {
	case pdata.MetricDataTypeDoubleGauge, pdata.MetricDataTypeHistogram:
		// gauges don't need to be adjusted so no additional processing is necessary
		return true, 0
	default:
		return ma.adjustMetricTimeseries(metric)
	}
}

// Returns true if at least one of the metric's timeseries was adjusted and false if all of the
// timeseries are an initial occurrence or a reset. Additionally returns the number of timeseries
// dropped.
func (ma *MetricsAdjuster) adjustMetricTimeseries(metric *pdata.Metric) (bool, int) {
	dropped := 0
	switch metric.DataType() {
	case pdata.MetricDataTypeHistogram:
		dataPoints := metric.Histogram().DataPoints()
		filtered := make([]pdata.HistogramDataPoint, 0, dataPoints.Len())
		for i := 0; i < dataPoints.Len(); i++ {
			current := dataPoints.At(i)
			tsi := ma.tsm.get(metric, current.LabelsMap())
			if tsi.initial == nil {
				tsi.initial = current
				tsi.previous = current
				continue
			}

			// Otherwise, try to adjust the time series.
			if ma.adjustTimeseries(metric.DataType(), current, tsi.initial, tsi.previous) {
				tsi.previous = current
				filtered = append(filtered, current)
			} else {
				// reset timeseries.
				tsi.initial = current
				tsi.previous = current
				dropped++
			}
		}
		if len(filtered) != 0 {
			dataPoints.Resize(0)
			for _, okPoint := range filtered {
				dataPoints.Append(okPoint)
			}
		}
		return len(filtered) > 0, dropped

	case pdata.MetricDataTypeSummary:

	case pdata.MetricDataTypeDoubleGauge:
	}

	panic("Unimplemented")
}

// Returns true if 'current' was adjusted and false if 'current' is an the initial occurrence or a
// reset of the timeseries.
func (ma *MetricsAdjuster) adjustTimeseries(metricType pdata.MetricDataType, current, initial, previous *pdata.Metric) bool {
	if !ma.adjustPoints(metricType, current, initial, previous) {
		return false
	}

	switch metricType {
	case pdata.MetricDataTypeSummary:
		currentDataPoints := current.Summary().DataPoints()
		initialDataPoints := initial.Summary().DataPoints()
		for i := 0; i < currentDataPoints.Len(); i++ {
			currentDataPoints.At(i).SetStartTimestamp(initialDataPoints.At(i).StartTimestamp())
		}

	case pdata.MetricDataTypeHistogram:
		currentDataPoints := current.Histogram().DataPoints()
		initialDataPoints := initial.Histogram().DataPoints()
		for i := 0; i < currentDataPoints.Len(); i++ {
			currentDataPoints.At(i).SetStartTimestamp(initialDataPoints.At(i).StartTimestamp())
		}

	case pdata.MetricDataTypeDoubleGauge:
		currentDataPoints := current.DoubleGauge().DataPoints()
		initialDataPoints := initial.DoubleGauge().DataPoints()
		for i := 0; i < currentDataPoints.Len(); i++ {
			currentDataPoints.At(i).SetStartTimestamp(initialDataPoints.At(i).StartTimestamp())
		}
	}
	return true
}

// Note: There is an important, subtle point here. When a new timeseries or a reset is detected,
// current and initial are the same object. When initial == previous, the previous value/count/sum
// are all the initial value. When initial != previous, the previous value/count/sum has been
// adjusted wrt the initial value so both they must be combined to find the actual previous
// value/count/sum. This happens because the timeseries are updated in-place - if new copies of the
// timeseries were created instead, previous could be used directly but this would mean reallocating
// all of the metrics.
func (ma *MetricsAdjuster) adjustPoints(metricType pdata.MetricDataType, current, initial, previous *pdata.Metric) bool {
	switch metricType {
	case pdata.MetricDataTypeDoubleSum:
		currentDataPoints := current.DoubleSum().DataPoints()
		initialDataPoints := initial.DoubleSum().DataPoints()
		previousDataPoints := previous.DoubleSum().DataPoints()
		currentLen, initialLen, previousLen := currentDataPoints.Len(), initialDataPoints.Len(), previousDataPoints.Len()
		if currentLen != 1 || initialLen != 1 || previousLen != 1 {
			ma.logger.Info("Adjusting Points, all lengths should be 1",
				zap.Int("len(current)", currentLen), zap.Int("len(initial)", initialLen), zap.Int("len(previous)", previousLen))
			return true
		}
		currentValue := currentDataPoints.At(0).Value()
		initialValue := initialDataPoints.At(0).Value()
		previousValue := initialValue
		if initial != previous {
			previousValue += previousDataPoints.At(0).Value()
		}
		if currentValue < previousValue {
			// reset detected
			return false
		}
		currentDataPoints.At(0).SetValue(currentValue - initialValue)

	case pdata.MetricDataTypeHistogram:
		currentDataPoints := current.Histogram().DataPoints()
		initialDataPoints := initial.Histogram().DataPoints()
		previousDataPoints := previous.Histogram().DataPoints()
		currentLen, initialLen, previousLen := currentDataPoints.Len(), initialDataPoints.Len(), previousDataPoints.Len()
		if currentLen != 1 || initialLen != 1 || previousLen != 1 {
			ma.logger.Info("Adjusting Points, all lengths should be 1",
				zap.Int("len(current)", currentLen), zap.Int("len(initial)", initialLen), zap.Int("len(previous)", previousLen))
			return true
		}
		// note: sum of squared deviation not currently supported
		currentDist := currentDataPoints.At(0)
		initialDist := initialDataPoints.At(0)
		previousDist := previousDataPoints.At(0)
		previousCount := initialDist.Count()
		previousSum := initialDist.Sum()
		if initialDist.Count() != previousDist.Count() || initialDist.Sum() != previousDist.Sum() {
			previousCount += previousDist.Count()
			previousSum += previousDist.Sum()
		}
		if currentDist.Count() < previousCount || currentDist.Sum() < previousSum {
			// reset detected
			return false
		}
		currentDist.SetCount(currentDist.Count() - initialDist.Count())
		currentDist.SetSum(currentDist.Sum() - initialDist.Sum())
		ma.adjustBuckets(currentDataPoints, initialDataPoints)

	case pdata.MetricDataTypeSummary:
		currentDataPoints := current.Summary().DataPoints()
		initialDataPoints := initial.Summary().DataPoints()
		previousDataPoints := previous.Summary().DataPoints()
		currentLen, initialLen, previousLen := currentDataPoints.Len(), initialDataPoints.Len(), previousDataPoints.Len()
		if currentLen != 1 || initialLen != 1 || previousLen != 1 {
			ma.logger.Info("Adjusting Points, all lengths should be 1",
				zap.Int("len(current)", currentLen), zap.Int("len(initial)", initialLen), zap.Int("len(previous)", previousLen))
			return true
		}
		// note: for summary, we don't adjust the snapshot
		currentSumy := currentDataPoints.At(0)
		initialSumy := initialDataPoints.At(0)
		previousSumy := previousDataPoints.At(0)
		currentCount := currentSumy.Count()
		currentSum := currentSumy.Sum()
		initialCount := initialSumy.Count()
		initialSum := initialSumy.Sum()
		previousCount := initialCount
		previousSum := initialSum
		if initial != previous {
			previousCount += previousSumy.Count()
			previousSum += previousSumy.Sum()
		}
		if currentCount < previousCount || currentSum < previousSum {
			// reset detected
			return false
		}
		currentSumy.SetCount(currentCount - initialCount)
		currentSumy.SetSum(currentSum - initialSum)

	default:
		// this shouldn't happen
		ma.logger.Info("Adjust - skipping unexpected point", zap.String("type", metricType.String()))
	}
	return true
}

func (ma *MetricsAdjuster) adjustBuckets(current, initial pdata.HistogramDataPointSlice) {
	curLen, initialLen := current.Len(), initial.Len()
	if curLen != initialLen {
		// this shouldn't happen
		ma.logger.Info("Bucket sizes not equal", zap.Int("len(current)", curLen), zap.Int("len(initial)", initialLen))
		return
	}

	for i := 0; i < curLen; i++ {
		currentPoint := current.At(i)
		initialPoint := initial.At(i)
		currentPoint.SetCount(currentPoint.Count() - initialPoint.Count())
	}
}
