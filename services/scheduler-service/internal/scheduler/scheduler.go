package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"predixaai-backend/services/scheduler-service/internal/mcp"
	"predixaai-backend/services/scheduler-service/internal/monitor"
	"predixaai-backend/services/scheduler-service/internal/security"
	"predixaai-backend/services/scheduler-service/internal/storage"
)

type Registry struct {
	mu         sync.Mutex
	jobs       map[string]*Job
	queue      chan JobRun
	workers    int
	repo       *storage.Repository
	ctx        context.Context
	cancel     context.CancelFunc
	jobTimeout time.Duration
	limits     security.Limits
}

type Job struct {
	ruleID  string
	spec    RuleSpec
	adapter mcp.DbMcpAdapter
	stop    chan struct{}
}

type JobInfo struct {
	RuleID             string `json:"ruleId"`
	PollIntervalSecond int    `json:"pollIntervalSeconds"`
}

type JobRun struct {
	ruleID  string
	spec    RuleSpec
	adapter mcp.DbMcpAdapter
}

func NewRegistry(repo *storage.Repository, limits security.Limits, workers int, jobTimeout time.Duration) *Registry {
	ctx, cancel := context.WithCancel(context.Background())
	reg := &Registry{
		jobs:       map[string]*Job{},
		queue:      make(chan JobRun, 128),
		workers:    workers,
		repo:       repo,
		ctx:        ctx,
		cancel:     cancel,
		jobTimeout: jobTimeout,
		limits:     limits,
	}
	for i := 0; i < workers; i++ {
		go reg.worker()
	}
	return reg
}

func (r *Registry) Stop() {
	r.cancel()
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, job := range r.jobs {
		close(job.stop)
	}
	r.jobs = map[string]*Job{}
}

func (r *Registry) Schedule(ruleID string, spec RuleSpec, adapter mcp.DbMcpAdapter) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.jobs[ruleID]; ok {
		close(existing.stop)
	}
	job := &Job{ruleID: ruleID, spec: spec, adapter: adapter, stop: make(chan struct{})}
	r.jobs[ruleID] = job
	go r.runTicker(job)
}

func (r *Registry) Unschedule(ruleID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if job, ok := r.jobs[ruleID]; ok {
		close(job.stop)
		delete(r.jobs, ruleID)
	}
}

func (r *Registry) ListJobs() []JobInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	jobs := make([]JobInfo, 0, len(r.jobs))
	for id, job := range r.jobs {
		jobs = append(jobs, JobInfo{RuleID: id, PollIntervalSecond: job.spec.PollIntervalSeconds})
	}
	return jobs
}

func (r *Registry) runTicker(job *Job) {
	ticker := time.NewTicker(time.Duration(job.spec.PollIntervalSeconds) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			r.queue <- JobRun{ruleID: job.ruleID, spec: job.spec, adapter: job.adapter}
		case <-job.stop:
			return
		case <-r.ctx.Done():
			return
		}
	}
}

func (r *Registry) worker() {
	for {
		select {
		case run := <-r.queue:
			r.execute(run)
		case <-r.ctx.Done():
			return
		}
	}
}

func (r *Registry) execute(run JobRun) {
	ctx, cancel := context.WithTimeout(context.Background(), r.jobTimeout)
	defer cancel()
	params := normalizeParameters(run.spec)
	if len(params) == 0 {
		return
	}
	for _, param := range params {
		result, err := r.evaluateParameter(ctx, run.spec, param, run.adapter)
		if err != nil || !result.Hit {
			continue
		}
		cooldown := 0
		if run.spec.CooldownSeconds != nil {
			cooldown = *run.spec.CooldownSeconds
		}
		if cooldown > 0 {
			lastAlert, err := r.repo.GetLastAlertForKey(ctx, run.ruleID, param.ParameterName, param.Detector.Type)
			if err == nil && monitor.WithinCooldown(lastAlert, cooldown) {
				continue
			}
		}
		metadataMap := map[string]any{
			"table":           run.spec.Source.Table,
			"valueColumn":     param.ValueColumn,
			"timestampColumn": run.spec.Source.TimestampColumn,
			"detector":        param.Detector.Type,
		}
		for k, v := range result.Metadata {
			metadataMap[k] = v
		}
		metadataMap["explain"] = buildExplain(result, param)
		metadata, _ := json.Marshal(metadataMap)
		_ = r.repo.CreateAlert(ctx, storage.AlertRecord{
			RuleID:         run.ruleID,
			TSUTC:          time.Now().UTC(),
			ParameterName:  param.ParameterName,
			ObservedValue:  result.Observed,
			LimitExpr:      result.LimitExpr,
			DetectorType:   param.Detector.Type,
			Severity:       result.Severity,
			AnomalyScore:   result.AnomalyScore,
			BaselineMedian: result.BaselineMedian,
			BaselineMAD:    result.BaselineMAD,
			Hit:            true,
			Treated:        false,
			Metadata:       metadata,
		})
	}
}

func (r *Registry) evaluateParameter(ctx context.Context, spec RuleSpec, param ParameterSpec, adapter mcp.DbMcpAdapter) (DetectorResult, error) {
	if adapter == nil {
		return DetectorResult{}, errors.New("adapter not configured")
	}
	switch param.Detector.Type {
	case "missing_data":
		if param.Detector.MissingData == nil {
			return DetectorResult{}, errors.New("missing_data detector missing config")
		}
		queryCtx, cancel := context.WithTimeout(ctx, r.limits.MaxQueryDuration)
		defer cancel()
		resp, err := adapter.QueryLatestValue(queryCtx, mcp.LatestValueRequest{
			ConnectionRef:   spec.ConnectionRef,
			Table:           spec.Source.Table,
			ValueColumn:     spec.Source.TimestampColumn,
			TimestampColumn: spec.Source.TimestampColumn,
			Where:           toWhere(spec.Source.Where),
		})
		if err != nil {
			if strings.Contains(err.Error(), "no rows") {
				return EvaluateMissingData(time.Time{}, param.Detector.MissingData.MaxGapSeconds, time.Now().UTC()), nil
			}
			return DetectorResult{}, err
		}
		timestamp, err := parseTimeValue(resp.Value)
		if err != nil {
			timestamp, err = parseTimeValue(resp.TS)
			if err != nil {
				return DetectorResult{}, err
			}
		}
		return EvaluateMissingData(timestamp, param.Detector.MissingData.MaxGapSeconds, time.Now().UTC()), nil
	case "robust_zscore":
		if param.Detector.RobustZ == nil {
			return DetectorResult{}, errors.New("robust_zscore detector missing config")
		}
		baseline := param.Detector.RobustZ.BaselineWindowSeconds
		since := time.Now().Add(-time.Duration(baseline) * time.Second).UTC().Format(time.RFC3339)
		queryCtx, cancel := context.WithTimeout(ctx, r.limits.MaxQueryDuration)
		defer cancel()
		rows, err := adapter.FetchRecentRows(queryCtx, mcp.FetchRecentRowsRequest{
			ConnectionRef:   spec.ConnectionRef,
			Table:           spec.Source.Table,
			Columns:         []string{param.ValueColumn, spec.Source.TimestampColumn},
			TimestampColumn: spec.Source.TimestampColumn,
			Where:           toWhere(spec.Source.Where),
			Since:           since,
			Limit:           r.limits.MaxSampleRows,
		})
		if err != nil {
			return DetectorResult{}, err
		}
		if len(rows.Rows) < param.Detector.RobustZ.MinSamples {
			return DetectorResult{Hit: false}, nil
		}
		samples := make([]float64, 0, len(rows.Rows))
		latest := 0.0
		for i, row := range rows.Rows {
			val, ok := row[param.ValueColumn]
			if !ok {
				continue
			}
			floatVal, err := toFloat(val)
			if err != nil {
				continue
			}
			if i == 0 {
				latest = floatVal
			}
			samples = append(samples, floatVal)
		}
		if len(samples) < param.Detector.RobustZ.MinSamples {
			return DetectorResult{Hit: false}, nil
		}
		result := EvaluateRobustZ(samples, latest, param.Detector.RobustZ.ZWarn, param.Detector.RobustZ.ZCrit)
		return result, nil
	default:
		if param.Detector.Threshold == nil {
			return DetectorResult{}, errors.New("threshold detector missing config")
		}
		if spec.Aggregation != "" && spec.Aggregation != "latest" && spec.WindowSeconds != nil {
			queryCtx, cancel := context.WithTimeout(ctx, r.limits.MaxQueryDuration)
			defer cancel()
			resp, err := adapter.QueryAggregate(queryCtx, mcp.AggregateRequest{
				ConnectionRef:   spec.ConnectionRef,
				Table:           spec.Source.Table,
				ValueColumn:     param.ValueColumn,
				TimestampColumn: spec.Source.TimestampColumn,
				Where:           toWhere(spec.Source.Where),
				Agg:             spec.Aggregation,
				WindowSeconds:   *spec.WindowSeconds,
			})
			if err != nil {
				return DetectorResult{}, err
			}
			return EvaluateThresholdDetector(*param.Detector.Threshold, resp.Value)
		}
		queryCtx, cancel := context.WithTimeout(ctx, r.limits.MaxQueryDuration)
		defer cancel()
		resp, err := adapter.QueryLatestValue(queryCtx, mcp.LatestValueRequest{
			ConnectionRef:   spec.ConnectionRef,
			Table:           spec.Source.Table,
			ValueColumn:     param.ValueColumn,
			TimestampColumn: spec.Source.TimestampColumn,
			Where:           toWhere(spec.Source.Where),
		})
		if err != nil {
			return DetectorResult{}, err
		}
		return EvaluateThresholdDetector(*param.Detector.Threshold, resp.Value)
	}
}

func toWhere(spec *WhereSpec) *mcp.WhereSpec {
	if spec == nil {
		return nil
	}
	clauses := make([]mcp.WhereClause, 0, len(spec.Clauses))
	for _, c := range spec.Clauses {
		clauses = append(clauses, mcp.WhereClause{Column: c.Column, Op: c.Op, Value: c.Value})
	}
	return &mcp.WhereSpec{Type: spec.Type, Clauses: clauses}
}

func normalizeParameters(spec RuleSpec) []ParameterSpec {
	if len(spec.Parameters) > 0 {
		for i := range spec.Parameters {
			if spec.Parameters[i].ParameterName == "" {
				spec.Parameters[i].ParameterName = spec.Parameters[i].ValueColumn
			}
		}
		return spec.Parameters
	}
	if spec.Source.ValueColumn == "" {
		return nil
	}
	return []ParameterSpec{{
		ParameterName: fallbackParamName(spec.ParameterName, spec.Source.ValueColumn),
		ValueColumn:   spec.Source.ValueColumn,
		Detector: DetectorSpec{
			Type: "threshold",
			Threshold: &ThresholdSpec{
				Op:    spec.Condition.Op,
				Value: spec.Condition.Value,
				Min:   spec.Condition.Min,
				Max:   spec.Condition.Max,
			},
		},
	}}
}

func fallbackParamName(name, valueColumn string) string {
	if name != "" {
		return name
	}
	return valueColumn
}

func buildExplain(result DetectorResult, param ParameterSpec) string {
	switch param.Detector.Type {
	case "robust_zscore":
		if result.AnomalyScore == nil || result.BaselineMedian == nil || result.BaselineMAD == nil {
			return "robust_zscore"
		}
		return fmt.Sprintf("robust_zscore=%.2f (warn>=%.2f, crit>=%.2f), median=%.2f, mad=%.2f", *result.AnomalyScore, param.Detector.RobustZ.ZWarn, param.Detector.RobustZ.ZCrit, *result.BaselineMedian, *result.BaselineMAD)
	case "missing_data":
		return fmt.Sprintf("missing_data max_gap=%ds", param.Detector.MissingData.MaxGapSeconds)
	case "threshold":
		return result.LimitExpr
	default:
		return "detector"
	}
}
