package util

import (
	"fmt"
	"testing"
)

func TestFromToday(t *testing.T) {
	days, err := FromToday("20060102", "20230216")
	fmt.Println(days)
	fmt.Println(fmt.Sprintf("%+v", err))
}
