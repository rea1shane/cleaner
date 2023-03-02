package rule

import "time"

type Rule interface {
	IsMatch(t time.Time, ts []time.Time) bool
}
