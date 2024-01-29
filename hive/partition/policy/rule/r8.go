package rule

import "time"

// R8 分区为该表的最新的 31 个分区之一
type R8 struct{}

func (r R8) IsMatch(t time.Time, ts []time.Time) bool {
	if len(ts) <= 31 {
		return true
	}
	return t.After(ts[len(ts)-32])
}
