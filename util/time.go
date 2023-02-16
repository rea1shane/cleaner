package util

import (
	"github.com/morikuni/failure"
	"time"
)

// FromToday 计算指定日期距今天的天数差
// 计算时会舍弃时分秒，只对比日期
// 如果是过去返回负数，未来返回正数
func FromToday(layout, value string) (int, error) {
	targetTime, err := ParseTime(layout, value)
	if err != nil {
		return 0, err
	}
	targetDate := GetZeroTime(targetTime)
	todayDate := GetZeroTime(time.Now())
	return int(targetDate.Sub(todayDate).Hours() / 24), nil
}

// GetZeroTime 获得时间所属日期的 0 点时间
func GetZeroTime(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
}

// ParseTime 用本地时区解析时间
func ParseTime(layout, value string) (time.Time, error) {
	t, err := time.ParseInLocation(layout, value, time.Now().Location())
	return t, failure.Wrap(err)
}
