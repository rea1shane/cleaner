package util

import (
	"fmt"
	"testing"
)

func TestFromToday(t *testing.T) {
	fmt.Println(FromToday("20060102", "20230217"))
}
