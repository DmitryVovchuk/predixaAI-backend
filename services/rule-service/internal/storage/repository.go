package storage

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type Repository struct {
	Store *Store
}

func NewRepository(store *Store) *Repository {
	return &Repository{Store: store}
}

func (r *Repository) CreateConnection(ctx context.Context, conn DBConnection) (string, error) {
	id := uuid.NewString()
	_, err := r.Store.Pool.Exec(ctx, `
		INSERT INTO db_connections (id, name, type, host, port, user_name, password_enc, database, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,now())`,
		id, conn.Name, conn.Type, conn.Host, conn.Port, conn.User, conn.Password, conn.Database,
	)
	if err != nil {
		return "", err
	}
	return id, nil
}

func (r *Repository) GetRule(ctx context.Context, id string) (RuleRecord, error) {
	row := r.Store.Pool.QueryRow(ctx, `
		SELECT id, name, description, connection_ref, parameter_name, rule_json, enabled, status, last_error, last_validated_at, created_at, updated_at
		FROM rules WHERE id=$1`, id)
	var rec RuleRecord
	if err := row.Scan(&rec.ID, &rec.Name, &rec.Description, &rec.ConnectionRef, &rec.ParameterName, &rec.RuleJSON, &rec.Enabled, &rec.Status, &rec.LastError, &rec.LastValidatedAt, &rec.CreatedAt, &rec.UpdatedAt); err != nil {
		return RuleRecord{}, ErrNotFound
	}
	return rec, nil
}

func (r *Repository) ListRules(ctx context.Context) ([]RuleRecord, error) {
	rows, err := r.Store.Pool.Query(ctx, `
		SELECT id, name, description, connection_ref, parameter_name, rule_json, enabled, status, last_error, last_validated_at, created_at, updated_at
		FROM rules ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	results := []RuleRecord{}
	for rows.Next() {
		var rec RuleRecord
		if err := rows.Scan(&rec.ID, &rec.Name, &rec.Description, &rec.ConnectionRef, &rec.ParameterName, &rec.RuleJSON, &rec.Enabled, &rec.Status, &rec.LastError, &rec.LastValidatedAt, &rec.CreatedAt, &rec.UpdatedAt); err != nil {
			return nil, err
		}
		results = append(results, rec)
	}
	return results, nil
}

func (r *Repository) CreateRule(ctx context.Context, rec RuleRecord) (string, error) {
	id := uuid.NewString()
	_, err := r.Store.Pool.Exec(ctx, `
		INSERT INTO rules (id, name, description, connection_ref, parameter_name, rule_json, enabled, status, last_error, last_validated_at, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,now(),now())`,
		id, rec.Name, rec.Description, rec.ConnectionRef, rec.ParameterName, rec.RuleJSON, rec.Enabled, rec.Status, rec.LastError, rec.LastValidatedAt,
	)
	if err != nil {
		return "", err
	}
	return id, nil
}

func (r *Repository) UpdateRule(ctx context.Context, rec RuleRecord) error {
	_, err := r.Store.Pool.Exec(ctx, `
		UPDATE rules
		SET name=$1, description=$2, parameter_name=$3, rule_json=$4, enabled=$5, status=$6, last_error=$7, last_validated_at=$8, updated_at=now()
		WHERE id=$9`,
		rec.Name, rec.Description, rec.ParameterName, rec.RuleJSON, rec.Enabled, rec.Status, rec.LastError, rec.LastValidatedAt, rec.ID,
	)
	return err
}

func (r *Repository) SetRuleEnabled(ctx context.Context, id string, enabled bool, status string) error {
	_, err := r.Store.Pool.Exec(ctx, `UPDATE rules SET enabled=$1, status=$2, updated_at=now() WHERE id=$3`, enabled, status, id)
	return err
}

func (r *Repository) ListAlerts(ctx context.Context, ruleID string) ([]AlertRecord, error) {
	rows, err := r.Store.Pool.Query(ctx, `
		SELECT id, rule_id, ts_utc, parameter_name, observed_value, limit_expression, hit, treated, metadata
		FROM alerts WHERE rule_id=$1 ORDER BY ts_utc DESC`, ruleID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	results := []AlertRecord{}
	for rows.Next() {
		var rec AlertRecord
		if err := rows.Scan(&rec.ID, &rec.RuleID, &rec.TSUTC, &rec.ParameterName, &rec.ObservedValue, &rec.LimitExpr, &rec.Hit, &rec.Treated, &rec.Metadata); err != nil {
			return nil, err
		}
		results = append(results, rec)
	}
	return results, nil
}

func (r *Repository) UpdateAlertTreated(ctx context.Context, alertID int64, treated bool) error {
	_, err := r.Store.Pool.Exec(ctx, `UPDATE alerts SET treated=$1 WHERE id=$2`, treated, alertID)
	return err
}

func (r *Repository) CreateAlert(ctx context.Context, alert AlertRecord) error {
	_, err := r.Store.Pool.Exec(ctx, `
		INSERT INTO alerts (rule_id, ts_utc, parameter_name, observed_value, limit_expression, hit, treated, metadata)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		alert.RuleID, alert.TSUTC, alert.ParameterName, alert.ObservedValue, alert.LimitExpr, alert.Hit, alert.Treated, alert.Metadata,
	)
	return err
}

func nowPtr() *time.Time {
	now := time.Now().UTC()
	return &now
}
