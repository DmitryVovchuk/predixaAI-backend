package storage

import "time"

type RuleRecord struct {
	ID            string
	ConnectionRef string
	RuleJSON      []byte
	Enabled       bool
	Status        string
	LastError     []byte
	LastValidated *time.Time
}

type AlertRecord struct {
	RuleID        string
	TSUTC         time.Time
	ParameterName string
	ObservedValue string
	LimitExpr     string
	Hit           bool
	Treated       bool
	Metadata      []byte
}
