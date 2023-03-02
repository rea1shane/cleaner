package rule

import (
	"github.com/rea1shane/cleaner/util"
	"time"
)

// R1 日期距当前日期小于等于 31 天。
type R1 struct{}

func (r R1) IsMatch(t time.Time, _ []time.Time) bool {
	return util.FromToday(t) >= -31
}
