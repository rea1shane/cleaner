package rule

import "time"

type Rule interface {
	IsMatch(t time.Time, ts []time.Time) bool
}

// FromToday 计算指定日期距今天的天数差
// 计算时会舍弃时分秒，只对比日期
// 如果是过去返回负数，未来返回正数
func FromToday(t time.Time) int {
	date := getZeroTime(t)
	todayDate := getZeroTime(time.Now())
	return int(date.Sub(todayDate).Hours() / 24)
}

// getZeroTime 获得时间所属日期的 0 点时间
func getZeroTime(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
}
