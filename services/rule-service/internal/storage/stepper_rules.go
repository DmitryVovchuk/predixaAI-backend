package storage

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type scanner interface {
	Scan(dest ...any) error
}

func (r *Repository) CreateStepperRule(ctx context.Context, rec StepperRule) (StepperRule, error) {
	id := uuid.NewString()
	row := r.Store.Pool.QueryRow(ctx, `
		INSERT INTO ui_rules (id, unit_id, name, rule_type, parameter_id, config, enabled, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,now(),now())
		RETURNING id, unit_id, name, rule_type, parameter_id, config, enabled, created_at, updated_at`,
		id, rec.UnitID, rec.Name, rec.RuleType, rec.ParameterID, rec.Config, rec.Enabled,
	)
	return scanStepperRule(row)
}

func (r *Repository) UpdateStepperRule(ctx context.Context, rec StepperRule) (StepperRule, error) {
	row := r.Store.Pool.QueryRow(ctx, `
		UPDATE ui_rules
		SET name=$1, rule_type=$2, parameter_id=$3, config=$4, enabled=$5, updated_at=now()
		WHERE id=$6
		RETURNING id, unit_id, name, rule_type, parameter_id, config, enabled, created_at, updated_at`,
		rec.Name, rec.RuleType, rec.ParameterID, rec.Config, rec.Enabled, rec.ID,
	)
	return scanStepperRule(row)
}

func (r *Repository) SetStepperRuleEnabled(ctx context.Context, id string, enabled bool) error {
	_, err := r.Store.Pool.Exec(ctx, `UPDATE ui_rules SET enabled=$1, updated_at=now() WHERE id=$2`, enabled, id)
	return err
}

func (r *Repository) DeleteStepperRule(ctx context.Context, id string) error {
	_, err := r.Store.Pool.Exec(ctx, `DELETE FROM ui_rules WHERE id=$1`, id)
	return err
}

func (r *Repository) GetStepperRule(ctx context.Context, id string) (StepperRule, error) {
	row := r.Store.Pool.QueryRow(ctx, `
		SELECT id, unit_id, name, rule_type, parameter_id, config, enabled, created_at, updated_at
		FROM ui_rules WHERE id=$1`, id)
	return scanStepperRule(row)
}

func (r *Repository) ListStepperRules(ctx context.Context, unitID string) ([]StepperRule, error) {
	query := `SELECT id, unit_id, name, rule_type, parameter_id, config, enabled, created_at, updated_at FROM ui_rules`
	args := []any{}
	if unitID != "" {
		query += " WHERE unit_id=$1"
		args = append(args, unitID)
	}
	query += " ORDER BY created_at DESC"
	rows, err := r.Store.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	results := []StepperRule{}
	for rows.Next() {
		rec, err := scanStepperRule(rows)
		if err != nil {
			return nil, err
		}
		results = append(results, rec)
	}
	return results, nil
}

func scanStepperRule(row scanner) (StepperRule, error) {
	var rec StepperRule
	var cfg json.RawMessage
	if err := row.Scan(&rec.ID, &rec.UnitID, &rec.Name, &rec.RuleType, &rec.ParameterID, &cfg, &rec.Enabled, &rec.CreatedAt, &rec.UpdatedAt); err != nil {
		return StepperRule{}, ErrNotFound
	}
	rec.Config = cfg
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = time.Now().UTC()
	}
	if rec.UpdatedAt.IsZero() {
		rec.UpdatedAt = rec.CreatedAt
	}
	return rec, nil
}
