package rule

import (
	"github.com/rea1shane/cleaner/util"
	"time"
)

// R3 判断日期是否是当前日期。
type R3 struct{}

func (r R3) IsMatch(t time.Time) bool {
	return util.FromToday(t) == 0
}
