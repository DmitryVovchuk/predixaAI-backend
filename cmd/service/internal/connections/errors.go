package connections

import "errors"

var (
	ErrInvalidInput  = errors.New("invalid input")
	ErrNotFound      = errors.New("connection not found")
	ErrNotConfigured = errors.New("connectionRef lookup not configured")
)
