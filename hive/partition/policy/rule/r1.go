package rule

import (
	"github.com/rea1shane/cleaner/util"
	"time"
)

// R1 判断日期是否是所属月的最后一天。
type R1 struct{}

func (r R1) IsMatch(t time.Time) bool {
	return util.IsMonthEnd(t)
}
