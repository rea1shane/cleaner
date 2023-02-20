package rule

import (
	"github.com/rea1shane/cleaner/util"
	"time"
)

// R2 判断日期是否距当前日期小于 365 天。
type R2 struct{}

func (r R2) IsMatch(t time.Time) bool {
	fromToday := util.FromToday(t)
	return fromToday > -365 && fromToday <= 0
}
