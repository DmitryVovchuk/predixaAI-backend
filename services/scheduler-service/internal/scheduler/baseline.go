package scheduler

import (
	"errors"
	"time"
)

const defaultBaselineLastN = 50

func buildBaselineWindow(now time.Time, baseline BaselineSpec, maxRows int) (time.Time, *time.Time, *time.Time, int, error) {
	if baseline.LastN == nil && baseline.TimeRange == nil {
		lastN := defaultBaselineLastN
		return now.Add(-time.Hour * 24 * 365), nil, nil, clampLimit(lastN, maxRows), nil
	}
	if baseline.TimeRange != nil {
		start, end, err := parseTimeRange(*baseline.TimeRange)
		if err != nil {
			return time.Time{}, nil, nil, 0, err
		}
		since := start
		return since, &start, &end, maxRows, nil
	}
	if baseline.LastN == nil || *baseline.LastN <= 0 {
		return time.Time{}, nil, nil, 0, errors.New("lastN must be > 0")
	}
	return now.Add(-time.Hour * 24 * 365), nil, nil, clampLimit(*baseline.LastN, maxRows), nil
}

func parseTimeRange(spec TimeRangeSpec) (time.Time, time.Time, error) {
	start, err := time.Parse(time.RFC3339, spec.Start)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	end, err := time.Parse(time.RFC3339, spec.End)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	if end.Before(start) {
		return time.Time{}, time.Time{}, errors.New("end must be after start")
	}
	return start, end, nil
}

func clampLimit(value int, maxRows int) int {
	if maxRows == 0 || value <= maxRows {
		return value
	}
	return maxRows
}
