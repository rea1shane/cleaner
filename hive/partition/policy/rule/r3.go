package rule

import (
	"time"
)

// R3 日期是所属月的最后一天。
type R3 struct{}

func (r R3) IsMatch(t time.Time, _ []time.Time) bool {
	return t.Month() != t.Add(24*time.Hour).Month()
}
