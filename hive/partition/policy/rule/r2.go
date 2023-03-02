package rule

import (
	"time"
)

// R2 日期距当前日期小于等于 365 天。
type R2 struct{}

func (r R2) IsMatch(t time.Time, _ []time.Time) bool {
	return FromToday(t) >= -365
}
