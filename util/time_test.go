package util

import (
	"fmt"
	"testing"
)

func TestIsMonthEnd(t *testing.T) {
	res, err := IsMonthEnd("20060102", "20230131")
	fmt.Println(res)
	fmt.Println(fmt.Sprintf("%+v", err))
}

func TestFromToday(t *testing.T) {
	days, err := FromToday("20060102", "20230216")
	fmt.Println(days)
	fmt.Println(fmt.Sprintf("%+v", err))
}
