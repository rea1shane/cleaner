package util

import (
	"github.com/morikuni/failure"
	"time"
)

// IsMonthEnd 判断时间是否是所属月的最后一天
func IsMonthEnd(t time.Time) bool {
	return t.Month() != t.Add(24*time.Hour).Month()
}

// FromToday 计算指定日期距今天的天数差
// 计算时会舍弃时分秒，只对比日期
// 如果是过去返回负数，未来返回正数
func FromToday(t time.Time) int {
	date := getZeroTime(t)
	todayDate := getZeroTime(time.Now())
	return int(date.Sub(todayDate).Hours() / 24)
}

// ParseTimes 批量解析时间
// 返回正确解析的时间数组、解析失败的字符串将及其错误原因
func ParseTimes(layout string, values []string) (tm map[string]time.Time, em map[string]error) {
	for _, v := range values {
		t, err := ParseTime(layout, v)
		if err != nil {
			if em == nil {
				em = make(map[string]error)
			}
			em[v] = err
		} else {
			if tm == nil {
				tm = make(map[string]time.Time)
			}
			tm[v] = t
		}
	}
	return
}

// ParseTime 用本地时区解析时间
func ParseTime(layout, value string) (time.Time, error) {
	t, err := time.ParseInLocation(layout, value, time.Now().Location())
	return t, failure.Wrap(err)
}

// getZeroTime 获得时间所属日期的 0 点时间
func getZeroTime(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
}
