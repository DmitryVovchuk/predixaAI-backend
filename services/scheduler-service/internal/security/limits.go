package security

import "time"

type Limits struct {
	MinPollSeconds    int
	MaxPollSeconds    int
	MaxWindowSeconds  int
	MaxQueryDuration  time.Duration
	MaxConcurrentCalls int
	MaxResultSize     int
}

func DefaultLimits() Limits {
	return Limits{
		MinPollSeconds:    5,
		MaxPollSeconds:    3600,
		MaxWindowSeconds:  86400,
		MaxQueryDuration:  5 * time.Second,
		MaxConcurrentCalls: 8,
		MaxResultSize:     1000,
	}
}
