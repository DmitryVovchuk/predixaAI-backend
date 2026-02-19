package scheduler

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"time"
)

const defaultEpsilon = 1e-9

type DetectorResult struct {
	Hit            bool
	Status         string
	Severity       string
	Observed       string
	LimitExpr      string
	Metadata       map[string]any
	AnomalyScore   *float64
	BaselineMedian *float64
	BaselineMAD    *float64
	WindowStart    *time.Time
	WindowEnd      *time.Time
	BaselineStart  *time.Time
	BaselineEnd    *time.Time
	Violations     []Violation
}

type Violation struct {
	Timestamp  *time.Time `json:"timestamp,omitempty"`
	Index      *int       `json:"index,omitempty"`
	Value      float64    `json:"value"`
	Reason     string     `json:"reason"`
	LimitName  string     `json:"limitName"`
	LimitValue float64    `json:"limitValue"`
	Delta      float64    `json:"delta"`
}

func EvaluateThresholdDetector(threshold ThresholdSpec, value any) (DetectorResult, error) {
	cond := ConditionSpec{Op: threshold.Op, Value: threshold.Value, Min: threshold.Min, Max: threshold.Max}
	hit, observed, expr := EvaluateCondition(cond, value)
	return DetectorResult{
		Hit:       hit,
		Status:    statusFromHit(hit),
		Severity:  "high",
		Observed:  observed,
		LimitExpr: expr,
	}, nil
}

func EvaluateRobustZ(samples []float64, latest float64, zWarn, zCrit float64) DetectorResult {
	median := Median(samples)
	mad := MAD(samples, median)
	result := DetectorResult{
		Hit:            false,
		Status:         statusOK,
		Severity:       "",
		Observed:       fmt.Sprint(latest),
		LimitExpr:      fmt.Sprintf("robust_zscore warn>=%.2f crit>=%.2f", zWarn, zCrit),
		AnomalyScore:   nil,
		BaselineMedian: &median,
		BaselineMAD:    &mad,
		Metadata:       map[string]any{},
	}
	if mad == 0 {
		if math.Abs(latest-median) <= defaultEpsilon {
			return result
		}
		score := math.Inf(1)
		result.AnomalyScore = &score
		result.Hit = true
		result.Status = statusViolation
		result.Severity = "high"
		return result
	}
	score := 0.6745 * (latest - median) / mad
	absScore := math.Abs(score)
	result.AnomalyScore = &score
	if absScore >= zCrit {
		result.Hit = true
		result.Status = statusViolation
		result.Severity = "high"
	} else if absScore >= zWarn {
		result.Hit = true
		result.Status = statusViolation
		result.Severity = "medium"
	}
	return result
}

func EvaluateMissingData(latestTS time.Time, maxGapSeconds int, now time.Time) DetectorResult {
	gap := now.Sub(latestTS)
	limit := time.Duration(maxGapSeconds) * time.Second
	result := DetectorResult{
		Hit:       gap > limit,
		Status:    statusFromHit(gap > limit),
		Severity:  "high",
		Observed:  latestTS.Format(time.RFC3339),
		LimitExpr: fmt.Sprintf("missing_data > %ds", maxGapSeconds),
		Metadata:  map[string]any{"gapSeconds": gap.Seconds()},
	}
	return result
}

const (
	statusOK              = "OK"
	statusViolation       = "VIOLATION"
	statusInsufficient    = "INSUFFICIENT_DATA"
	statusInvalidConfig   = "INVALID_CONFIG"
)

func statusFromHit(hit bool) string {
	if hit {
		return statusViolation
	}
	return statusOK
}

func Median(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)
	mid := len(sorted) / 2
	if len(sorted)%2 == 0 {
		return (sorted[mid-1] + sorted[mid]) / 2
	}
	return sorted[mid]
}

func MAD(values []float64, median float64) float64 {
	if len(values) == 0 {
		return 0
	}
	dev := make([]float64, len(values))
	for i, v := range values {
		dev[i] = math.Abs(v - median)
	}
	return Median(dev)
}

func parseTimeValue(value any) (time.Time, error) {
	switch t := value.(type) {
	case time.Time:
		return t, nil
	case string:
		if ts, err := time.Parse(time.RFC3339, t); err == nil {
			return ts, nil
		}
		if ts, err := time.Parse(time.RFC3339Nano, t); err == nil {
			return ts, nil
		}
		return time.Time{}, errors.New("unsupported timestamp")
	default:
		return time.Time{}, errors.New("unsupported timestamp")
	}
}
