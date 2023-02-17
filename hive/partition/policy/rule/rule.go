package rule

import "time"

type Rule interface {
	IsMatch(t time.Time) bool
}
