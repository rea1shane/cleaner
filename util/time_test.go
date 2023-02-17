package util

import (
	"fmt"
	"testing"
)

const (
	layout = "data_date=20060102"
)

func TestParseTimes(t *testing.T) {
	values := []string{
		"data_date=20230130",
		"data_date=20230131",
		"data_date=20230222",
		"data_date=20230230",
	}
	ts, em := ParseTimes(layout, values)
	for _, t := range ts {
		fmt.Println(t)
	}
	for s, err := range em {
		fmt.Println(s + ": " + err.Error())
	}
}

func TestIsMonthEnd(t *testing.T) {
	time, err := ParseTime(layout, "data_date=20230130")
	fmt.Println(fmt.Sprintf("%+v", err))
	fmt.Println(IsMonthEnd(time))
}

func TestFromToday(t *testing.T) {
	time, err := ParseTime(layout, "data_date=20230131")
	fmt.Println(fmt.Sprintf("%+v", err))
	fmt.Println(FromToday(time))
}
