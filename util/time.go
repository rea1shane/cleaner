package util

import (
	"github.com/morikuni/failure"
	"time"
)

const DateLayout = "2006-01-02"

// FromToday 计算指定日期距今天的天数差
// 计算时会舍弃时分秒，只对比日期
// 如果是过去返回负数，未来返回正数
func FromToday(layout, value string) (int, error) {
	targetTime, err := time.Parse(layout, value)
	if err != nil {
		return 0, failure.Wrap(err)
	}
	targetDate, _ := time.Parse(DateLayout, targetTime.Format(DateLayout))
	today, _ := time.Parse(DateLayout, time.Now().Format(DateLayout))
	return int(targetDate.Sub(today).Hours() / 24), nil
}
