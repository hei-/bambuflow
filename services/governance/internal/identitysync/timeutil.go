package identitysync

import "time"

func timeUntil(t time.Time) time.Duration {
	if d := time.Until(t); d > 0 {
		return d
	}
	return time.Millisecond
}
