package mod

import (
	"github.com/rea1shane/cleaner/hive/partition/policy/rule"
	"github.com/rea1shane/cleaner/util"
)

type Mod struct {
	rules []rule.Rule
}

func (m Mod) Group(layout string, values []string) (matched, unmatched, errorValue []string) {
	tm, em := util.ParseTimes(layout, values)
	for s := range em {
		errorValue = append(errorValue, s)
	}
	for s, t := range tm {
		isMatch := false
		for _, r := range m.rules {
			if r.IsMatch(t) {
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
