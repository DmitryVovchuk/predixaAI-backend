package storage

import (
	"encoding/json"
	"time"
)

type DBConnection struct {
	ID        string
	Name      string
	Type      string
	Host      string
	Port      int
	User      string
	Password  string
	Database  string
	CreatedAt time.Time
}

type RuleRecord struct {
	ID              string
	Name            string
	Description     string
	ConnectionRef   string
	ParameterName   string
	RuleJSON        []byte
	Enabled         bool
	Status          string
	LastError       []byte
	LastValidatedAt *time.Time
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

type AlertRecord struct {
	ID             int64
	RuleID         string
	TSUTC          time.Time
	ParameterName  string
	ObservedValue  string
	LimitExpr      string
	DetectorType   string
	Severity       string
	AnomalyScore   *float64
	BaselineMedian *float64
	BaselineMAD    *float64
	Hit            bool
	Treated        bool
	Metadata       []byte
}

type MachineUnit struct {
	UnitID          string
	UnitName        string
	ConnectionRef   string
	SelectedTable   string
	SelectedColumns []string
	LiveParameters  json.RawMessage
	RuleIDs         []string
	PosX            float64
	PosY            float64
	CreatedAt       time.Time
	UpdatedAt       time.Time
}
