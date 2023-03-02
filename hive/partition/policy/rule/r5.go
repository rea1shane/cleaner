package rule

import (
	"time"
)

// R5 分区是该表的最新分区。
type R5 struct{}

func (r R5) IsMatch(t time.Time, ts []time.Time) bool {
	for _, t2 := range ts {
		if t.Sub(t2) > 0 {
			return false
		}
	}
	return true
}
