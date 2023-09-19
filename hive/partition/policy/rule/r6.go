package rule

import (
	"time"
)

// R6 日期距当前日期小于等于 10 天。
type R6 struct{}

func (r R6) IsMatch(t time.Time, _ []time.Time) bool {
	return FromToday(t) >= -10
}
