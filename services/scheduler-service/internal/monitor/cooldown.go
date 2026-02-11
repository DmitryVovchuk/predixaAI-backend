package monitor

import "time"

func WithinCooldown(last time.Time, cooldownSeconds int) bool {
	return time.Since(last) < time.Duration(cooldownSeconds)*time.Second
}
