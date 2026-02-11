package storage

import (
	"context"
	"time"
)

type Repository struct {
	Store *Store
}

func NewRepository(store *Store) *Repository {
	return &Repository{Store: store}
}

func (r *Repository) ListEnabledRules(ctx context.Context) ([]RuleRecord, error) {
	rows, err := r.Store.Pool.Query(ctx, `
		SELECT id, connection_ref, rule_json, enabled, status, last_error, last_validated_at
		FROM rules WHERE enabled = true`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	results := []RuleRecord{}
	for rows.Next() {
		var rec RuleRecord
		if err := rows.Scan(&rec.ID, &rec.ConnectionRef, &rec.RuleJSON, &rec.Enabled, &rec.Status, &rec.LastError, &rec.LastValidated); err != nil {
			return nil, err
		}
		results = append(results, rec)
	}
	return results, nil
}

func (r *Repository) GetRule(ctx context.Context, id string) (RuleRecord, error) {
	row := r.Store.Pool.QueryRow(ctx, `
		SELECT id, connection_ref, rule_json, enabled, status, last_error, last_validated_at
		FROM rules WHERE id=$1`, id)
	var rec RuleRecord
	if err := row.Scan(&rec.ID, &rec.ConnectionRef, &rec.RuleJSON, &rec.Enabled, &rec.Status, &rec.LastError, &rec.LastValidated); err != nil {
		return RuleRecord{}, ErrNotFound
	}
	return rec, nil
}

func (r *Repository) GetConnectionType(ctx context.Context, id string) (string, error) {
	row := r.Store.Pool.QueryRow(ctx, `SELECT type FROM db_connections WHERE id=$1`, id)
	var connType string
	if err := row.Scan(&connType); err != nil {
		return "", ErrNotFound
	}
	return connType, nil
}

func (r *Repository) UpdateRuleStatus(ctx context.Context, id, status string, lastError []byte) error {
	_, err := r.Store.Pool.Exec(ctx, `
		UPDATE rules SET status=$1, last_error=$2, last_validated_at=now(), updated_at=now() WHERE id=$3`, status, lastError, id)
	return err
}

func (r *Repository) CreateAlert(ctx context.Context, alert AlertRecord) error {
	_, err := r.Store.Pool.Exec(ctx, `
		INSERT INTO alerts (rule_id, ts_utc, parameter_name, observed_value, limit_expression, hit, treated, metadata)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		alert.RuleID, alert.TSUTC, alert.ParameterName, alert.ObservedValue, alert.LimitExpr, alert.Hit, alert.Treated, alert.Metadata)
	return err
}

func (r *Repository) GetLastAlert(ctx context.Context, ruleID string) (time.Time, error) {
	row := r.Store.Pool.QueryRow(ctx, `SELECT ts_utc FROM alerts WHERE rule_id=$1 ORDER BY ts_utc DESC LIMIT 1`, ruleID)
	var ts time.Time
	if err := row.Scan(&ts); err != nil {
		return time.Time{}, ErrNotFound
	}
	return ts, nil
}
