package mod

import (
	"github.com/rea1shane/cleaner/hive/partition/policy/rule"
	"github.com/rea1shane/cleaner/util"
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
)

type Mod struct {
	rules []rule.Rule
}

func (m Mod) Group(layout string, partitions []string) (matched, unmatched, errorValue []string) {
	// 将分区转化为时间
	tm, em := util.ParseTimes(layout, partitions)
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
