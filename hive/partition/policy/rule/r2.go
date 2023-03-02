package rule

import (
	"github.com/rea1shane/cleaner/util"
	"time"
)

// R2 日期距当前日期小于等于 365 天。
type R2 struct{}

func (r R2) IsMatch(t time.Time, _ []time.Time) bool {
	return util.FromToday(t) >= -365
}
