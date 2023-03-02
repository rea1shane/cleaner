package rule

import "time"

// R4 所属月没有最后一天的分区。
type R4 struct{}

func (r R4) IsMatch(t time.Time, ts []time.Time) bool {
	var (
		monthEndNextDayYear  int
		monthEndNextDayMonth time.Month
	)
	switch t.Month() {
	case 12:
		monthEndNextDayYear = t.Year() + 1
		monthEndNextDayMonth = 1
	default:
		monthEndNextDayMonth = t.Month() + 1
	}
	monthEndNextDay := time.Date(monthEndNextDayYear, monthEndNextDayMonth, 1, 0, 0, 0, 0, t.Location())
	monthEnd := monthEndNextDay.Add(-24 * time.Hour)
	for _, t2 := range ts {
		if t2 == monthEnd {
			return false
		}
	}
	return true
}
