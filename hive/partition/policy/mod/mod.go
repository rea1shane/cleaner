package mod

import (
	"github.com/morikuni/failure"
	"github.com/rea1shane/cleaner/hive/partition/policy/rule"
	"time"
)

var (
	M1 = Mod{[]rule.Rule{
		rule.R1{},
		rule.R3{},
		rule.R4{},
		rule.R5{},
	}}
	M2 = Mod{[]rule.Rule{
		rule.R2{},
		rule.R3{},
		rule.R4{},
		rule.R5{},
	}}
	M3 = Mod{[]rule.Rule{
		rule.R3{},
		rule.R6{},
	}}
)

type Mod struct {
	rules []rule.Rule
}

func (m Mod) Group(layout string, partitions []string) (matched, unmatched, errorValue []string) {
	// 将分区转化为时间
	tm, em := parseTimes(layout, partitions)
	// 记录错误的分区
	for s := range em {
		errorValue = append(errorValue, s)
	}

	// 生成时间数组
	var ts []time.Time
	for _, t := range tm {
		ts = append(ts, t)
	}

	// 判断是否符合规则
	for s, t := range tm {
		isMatch := false
		for _, r := range m.rules {
			if r.IsMatch(t, ts) {
				isMatch = true
				break
			}
		}
		if isMatch {
			matched = append(matched, s)
		} else {
			unmatched = append(unmatched, s)
		}
	}
	return
}

// parseTimes 批量解析时间
// 返回正确解析的时间数组、解析失败的字符串将及其错误原因
func parseTimes(layout string, values []string) (tm map[string]time.Time, em map[string]error) {
	for _, v := range values {
		t, err := parseTime(layout, v)
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

// parseTime 用本地时区解析时间
func parseTime(layout, value string) (time.Time, error) {
	t, err := time.ParseInLocation(layout, value, time.Now().Location())
	return t, failure.Wrap(err)
}
