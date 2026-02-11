package storage

import "time"

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
	Hit            bool
	Treated        bool
	Metadata       []byte
}
