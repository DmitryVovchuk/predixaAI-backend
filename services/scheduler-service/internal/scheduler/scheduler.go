package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"predixaai-backend/services/scheduler-service/internal/monitor"
	"predixaai-backend/services/scheduler-service/internal/mcp"
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
	ruleID string
	spec   RuleSpec
	adapter mcp.DbMcpAdapter
	stop  chan struct{}
}

type JobInfo struct {
	RuleID             string `json:"ruleId"`
	PollIntervalSecond int    `json:"pollIntervalSeconds"`
}

type JobRun struct {
	ruleID string
	spec   RuleSpec
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

	value, expr, observed, err := r.fetchValue(ctx, run.spec, run.adapter)
	if err != nil {
		return
	}
	if !value {
		return
	}

	cooldown := 0
	if run.spec.CooldownSeconds != nil {
		cooldown = *run.spec.CooldownSeconds
	}
	if cooldown > 0 {
		lastAlert, err := r.repo.GetLastAlert(ctx, run.ruleID)
		if err == nil && monitor.WithinCooldown(lastAlert, cooldown) {
			return
		}
	}

	metadata, _ := json.Marshal(map[string]any{
		"table": run.spec.Source.Table,
		"valueColumn": run.spec.Source.ValueColumn,
	})

	_ = r.repo.CreateAlert(ctx, storage.AlertRecord{
		RuleID:        run.ruleID,
		TSUTC:         time.Now().UTC(),
		ParameterName: run.spec.ParameterName,
		ObservedValue: observed,
		LimitExpr:     expr,
		Hit:           true,
		Treated:       false,
		Metadata:      metadata,
	})
}

func (r *Registry) fetchValue(ctx context.Context, spec RuleSpec, adapter mcp.DbMcpAdapter) (bool, string, string, error) {
	if adapter == nil {
		return false, "", "", errors.New("adapter not configured")
	}
	if spec.Aggregation == "latest" {
		queryCtx, cancel := context.WithTimeout(ctx, r.limits.MaxQueryDuration)
		defer cancel()
		resp, err := adapter.QueryLatestValue(queryCtx, mcp.LatestValueRequest{
			ConnectionRef:   spec.ConnectionRef,
			Table:           spec.Source.Table,
			ValueColumn:     spec.Source.ValueColumn,
			TimestampColumn: spec.Source.TimestampColumn,
			Where:           toWhere(spec.Source.Where),
		})
		if err != nil {
			return false, "", "", err
		}
		hit, observed, expr := EvaluateCondition(spec.Condition, resp.Value)
		return hit, expr, observed, nil
	}
	if spec.WindowSeconds == nil {
		return false, "", "", errors.New("windowSeconds required")
	}
	queryCtx, cancel := context.WithTimeout(ctx, r.limits.MaxQueryDuration)
	defer cancel()
	resp, err := adapter.QueryAggregate(queryCtx, mcp.AggregateRequest{
		ConnectionRef:   spec.ConnectionRef,
		Table:           spec.Source.Table,
		ValueColumn:     spec.Source.ValueColumn,
		TimestampColumn: spec.Source.TimestampColumn,
		Where:           toWhere(spec.Source.Where),
		Agg:             spec.Aggregation,
		WindowSeconds:   *spec.WindowSeconds,
	})
	if err != nil {
		return false, "", "", err
	}
	hit, observed, expr := EvaluateCondition(spec.Condition, resp.Value)
	return hit, expr, observed, nil
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
