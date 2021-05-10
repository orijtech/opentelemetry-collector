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
	"sort"
	"strings"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/textparse"
	"github.com/prometheus/prometheus/scrape"

	"go.opentelemetry.io/collector/consumer/pdata"
)

// MetricFamily is unit which is corresponding to the metrics items which shared the same TYPE/UNIT/... metadata from
// a single scrape.
type MetricFamily interface {
	Add(metricName string, ls labels.Labels, t int64, v float64) error
	IsSameFamily(metricName string) bool
	ToMetric() (*pdata.Metric, int, int)
}

type metricFamily struct {
	name              string
	mtype             pdata.MetricDataType
	mc                MetadataCache
	droppedTimeseries int
	labelKeys         map[string]bool
	labelKeysOrdered  []string
	metadata          *scrape.MetricMetadata
	groupOrders       map[string]int
	groups            map[string]*metricGroup
}

func newMetricFamily(metricName string, mc MetadataCache) MetricFamily {
	familyName := normalizeMetricName(metricName)

	// lookup metadata based on familyName
	metadata, ok := mc.Metadata(familyName)
	if !ok && metricName != familyName {
		// use the original metricName as metricFamily
		familyName = metricName
		// perform a 2nd lookup with the original metric name. it can happen if there's a metric which is not histogram
		// or summary, but ends with one of those _count/_sum suffixes
		metadata, ok = mc.Metadata(metricName)
		// still not found, this can happen when metric has no TYPE HINT
		if !ok {
			metadata.Metric = familyName
			metadata.Type = textparse.MetricTypeUnknown
		}
	}

	return &metricFamily{
		name:              familyName,
		mtype:             convToPdataType(metadata.Type),
		mc:                mc,
		droppedTimeseries: 0,
		labelKeys:         make(map[string]bool),
		labelKeysOrdered:  make([]string, 0),
		metadata:          &metadata,
		groupOrders:       make(map[string]int),
		groups:            make(map[string]*metricGroup),
	}
}

func (mf *metricFamily) IsSameFamily(metricName string) bool {
	// trim known suffix if necessary
	familyName := normalizeMetricName(metricName)
	return mf.name == familyName || familyName != metricName && mf.name == metricName
}

// updateLabelKeys is used to store all the label keys of a same metric family in observed order. since prometheus
// receiver removes any label with empty value before feeding it to an appender, in order to figure out all the labels
// from the same metric family we will need to keep track of what labels have ever been observed.
func (mf *metricFamily) updateLabelKeys(ls labels.Labels) {
	for _, l := range ls {
		if isUsefulLabel(mf.mtype, l.Name) {
			if _, ok := mf.labelKeys[l.Name]; !ok {
				mf.labelKeys[l.Name] = true
				// use insertion sort to maintain order
				i := sort.SearchStrings(mf.labelKeysOrdered, l.Name)
				labelKeys := append(mf.labelKeysOrdered, "")
				copy(labelKeys[i+1:], labelKeys[i:])
				labelKeys[i] = l.Name
				mf.labelKeysOrdered = labelKeys
			}
		}
	}
}

func (mf *metricFamily) isCumulativeType() bool {
	return mf.mtype == pdata.MetricDataTypeDoubleSum ||
		mf.mtype == pdata.MetricDataTypeIntSum ||
		mf.mtype == pdata.MetricDataTypeHistogram ||
		mf.mtype == pdata.MetricDataTypeSummary
}

func (mf *metricFamily) getGroupKey(ls labels.Labels) string {
	mf.updateLabelKeys(ls)
	return dpgSignature(mf.labelKeysOrdered, ls)
}

// getGroups to return groups in insertion order
func (mf *metricFamily) getGroups() []*metricGroup {
	groups := make([]*metricGroup, len(mf.groupOrders))
	for k, v := range mf.groupOrders {
		groups[v] = mf.groups[k]
	}

	return groups
}

func (mf *metricFamily) loadMetricGroupOrCreate(groupKey string, ls labels.Labels, ts int64) *metricGroup {
	mg, ok := mf.groups[groupKey]
	if !ok {
		mg = &metricGroup{
			family:       mf,
			ts:           ts,
			ls:           ls,
			complexValue: make([]*dataPoint, 0),
		}
		mf.groups[groupKey] = mg
		// maintaining data insertion order is helpful to generate stable/reproducible metric output
		mf.groupOrders[groupKey] = len(mf.groupOrders)
	}
	return mg
}

func (mf *metricFamily) getLabelKeys(mg *metricGroup) pdata.StringMap {
	sm := pdata.NewStringMap()
	lmap := mg.ls.Map()
	for i := range mf.labelKeysOrdered {
		key := mf.labelKeysOrdered[i]
		value := lmap[key]
		sm.Upsert(key, value)
	}
	return sm
}

func isCummulativeType(typ pdata.MetricDataType) bool {
	return typ == pdata.MetricDataTypeHistogram || typ == pdata.MetricDataTypeSummary
}

func (mf *metricFamily) Add(metricName string, ls labels.Labels, t int64, v float64) error {
	groupKey := mf.getGroupKey(ls)
	mg := mf.loadMetricGroupOrCreate(groupKey, ls, t)
	if !isCummulativeType(mf.mtype) {
		mg.value = v
	} else {
		switch {
		case strings.HasSuffix(metricName, metricsSuffixSum):
			// always use the timestamp from sum (count is ok too), because the startTs from quantiles won't be reliable
			// in cases like remote server restart
			mg.ts = t
			mg.sum = v
			mg.hasSum = true
		case strings.HasSuffix(metricName, metricsSuffixCount):
			mg.count = v
			mg.hasCount = true
		default:
			boundary, err := getBoundary(mf.mtype, ls)
			if err != nil {
				mf.droppedTimeseries++
				return err
			}
			mg.complexValue = append(mg.complexValue, &dataPoint{value: v, boundary: boundary})
		}
	}

	return nil
}

func (mf *metricFamily) ToMetric() (*pdata.Metric, int, int) {
	metric := pdata.NewMetric()
	nDataPoints := 0

	switch mf.mtype {
	// not supported currently
	// case metricspb.MetricDescriptor_GAUGE_DISTRIBUTION:
	//	return nil
	case pdata.MetricDataTypeHistogram:
		metric.SetDataType(pdata.MetricDataTypeHistogram)
		dataPoints := metric.Histogram().DataPoints()
		for _, mg := range mf.getGroups() {
			dataPoint := dataPoints.AppendEmpty()
			if !mg.toDistributionDataPoint(&dataPoint, mf.labelKeysOrdered) {
				mf.droppedTimeseries++
			}
		}
		nDataPoints = dataPoints.Len()

	case pdata.MetricDataTypeSummary:
		metric.SetDataType(pdata.MetricDataTypeSummary)
		dataPoints := metric.Summary().DataPoints()
		for _, mg := range mf.getGroups() {
			dataPoint := dataPoints.AppendEmpty()
			if !mg.toSummaryDataPoint(&dataPoint, mf.labelKeysOrdered) {
				mf.droppedTimeseries++
			}
		}
		nDataPoints = dataPoints.Len()

	case pdata.MetricDataTypeDoubleSum:
		metric.SetDataType(pdata.MetricDataTypeDoubleSum)
		dataPoints := metric.DoubleSum().DataPoints()
		for _, mg := range mf.getGroups() {
			dataPoint := dataPoints.AppendEmpty()
			if !mg.toDoubleDataPoint(&dataPoint, mf.labelKeysOrdered) {
				mf.droppedTimeseries++
			}
		}
		nDataPoints = dataPoints.Len()

	default:
		metric.SetDataType(pdata.MetricDataTypeDoubleSum)
		dataPoints := metric.DoubleSum().DataPoints()
		for _, mg := range mf.getGroups() {
			dataPoint := dataPoints.AppendEmpty()
			if !mg.toDoubleDataPoint(&dataPoint, mf.labelKeysOrdered) {
				mf.droppedTimeseries++
			}
		}
		nDataPoints = dataPoints.Len()
	}

	// note: the total number of timeseries is the length of timeseries plus the number of dropped timeseries.
	if nDataPoints > 0 {
		return &metric, nDataPoints + mf.droppedTimeseries, mf.droppedTimeseries
	}
	return nil, mf.droppedTimeseries, mf.droppedTimeseries
}

type dataPoint struct {
	value    float64
	boundary float64
}

// metricGroup, represents a single metric of a metric family. for example a histogram metric is usually represent by
// a couple data complexValue (buckets and count/sum), a group of a metric family always share a same set of tags. for
// simple types like counter and gauge, each data point is a group of itself
type metricGroup struct {
	family       *metricFamily
	ts           int64
	ls           labels.Labels
	count        float64
	hasCount     bool
	sum          float64
	hasSum       bool
	value        float64
	complexValue []*dataPoint
}

func (mg *metricGroup) sortPoints() {
	sort.Slice(mg.complexValue, func(i, j int) bool {
		return mg.complexValue[i].boundary < mg.complexValue[j].boundary
	})
}

func (mg *metricGroup) toDistributionDataPoint(dataPoint *pdata.HistogramDataPoint, orderedLabelKeys []string) bool {
	if !mg.hasCount || len(mg.complexValue) == 0 {
		return false
	}
	mg.sortPoints()
	// for pdata and OCAgent Proto, the bounds won't include +inf
	bounds := make([]float64, len(mg.complexValue)-1)
	buckets := make([]uint64, len(mg.complexValue))

	for i := 0; i < len(mg.complexValue); i++ {
		if i != len(mg.complexValue)-1 {
			// not need to add +inf as bound to oc proto
			bounds[i] = mg.complexValue[i].boundary
		}
		adjustedCount := mg.complexValue[i].value
		if i != 0 {
			adjustedCount -= mg.complexValue[i-1].value
		}
		buckets[i] = uint64(adjustedCount)
	}

	dataPoint.SetCount(uint64(mg.count))
	dataPoint.SetExplicitBounds(bounds)
	dataPoint.SetSum(mg.sum)
	dataPoint.SetStartTimestamp(pdata.Timestamp(mg.ts))
	dataPoint.SetTimestamp(pdata.Timestamp(mg.ts))
	dataPoint.SetBucketCounts(buckets)

	return true
}

func (mg *metricGroup) toSummaryDataPoint(dataPoint *pdata.SummaryDataPoint, orderedLabelKeys []string) bool {
	// expecting count to be provided, however, in the following two cases, they can be missed.
	// 1. data is corrupted
	// 2. ignored by startValue evaluation
	if !mg.hasCount {
		return false
	}
	mg.sortPoints()

	// Based on the summary description from https://prometheus.io/docs/concepts/metric_types/#summary
	// the quantiles are calculated over a sliding time window, however, the count is the total count of
	// observations and the corresponding sum is a sum of all observed values, thus the sum and count used
	// at the global level of the metricspb.SummaryValue
	dataPoint.SetCount(uint64(mg.count))
	dataPoint.SetStartTimestamp(pdata.Timestamp(mg.ts))
	dataPoint.SetTimestamp(pdata.Timestamp(mg.ts))
	dataPoint.SetSum(mg.sum)
	pQuantileValues := dataPoint.QuantileValues()
	for _, p := range mg.complexValue {
		pQuantile := pQuantileValues.AppendEmpty()
		pQuantile.SetValue(p.value)
		pQuantile.SetQuantile(p.boundary * 100)
	}

	populateLabelValues(orderedLabelKeys, mg.ls, dataPoint.LabelsMap())

	return true
}

func (mg *metricGroup) toDoubleDataPoint(dataPoint *pdata.DoubleDataPoint, orderedLabelKeys []string) bool {
	mg.sortPoints()
	var startTs pdata.Timestamp
	// gauge/undefined types has no start time
	if mg.family.isCumulativeType() {
		startTs = pdata.Timestamp(mg.ts)
	}

	dataPoint.SetStartTimestamp(startTs)
	dataPoint.SetTimestamp(pdata.Timestamp(mg.ts))
	dataPoint.SetValue(mg.value)
	populateLabelValues(orderedLabelKeys, mg.ls, dataPoint.LabelsMap())

	return true
}

func populateLabelValues(orderedKeys []string, ls labels.Labels, sm pdata.StringMap) {
	lmap := ls.Map()
	for _, key := range orderedKeys {
		sm.Upsert(key, lmap[key])
	}
}
