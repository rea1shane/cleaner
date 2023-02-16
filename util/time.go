package util

import (
	"github.com/morikuni/failure"
	"time"
)

// IsMonthEnd 判断时间是否是所属月的最后一天
// 警告：必须处理异常，当有异常发生时 bool 的值为 false
func IsMonthEnd(layout, value string) (bool, error) {
	t, err := parseTime(layout, value)
	if err != nil {
		return false, err
	}
	nextDay := t.Add(24 * time.Hour)
	return t.Month() != nextDay.Month(), nil
}

// FromToday 计算指定日期距今天的天数差
// 计算时会舍弃时分秒，只对比日期
// 如果是过去返回负数，未来返回正数
func FromToday(layout, value string) (int, error) {
	targetTime, err := parseTime(layout, value)
	if err != nil {
		return 0, err
	}
	targetDate := getZeroTime(targetTime)
	todayDate := getZeroTime(time.Now())
	return int(targetDate.Sub(todayDate).Hours() / 24), nil
}

// getZeroTime 获得时间所属日期的 0 点时间
func getZeroTime(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
}

// parseTime 用本地时区解析时间
func parseTime(layout, value string) (time.Time, error) {
	t, err := time.ParseInLocation(layout, value, time.Now().Location())
	return t, failure.Wrap(err)
}
