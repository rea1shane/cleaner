package rule

import (
	"errors"
	"time"
)

// R7 分区为所属月最后一个分区
type R7 struct{}

func (r R7) IsMatch(t time.Time, ts []time.Time) bool {
	size := len(ts)
	for i := size - 1; i > 0; i-- {
		if ts[i].Year() == t.Year() && ts[i].Month() == t.Month() {
			return ts[i].Day() == t.Day()
		}
	}
	panic(errors.New("未知异常"))
}
