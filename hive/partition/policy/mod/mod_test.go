package mod

import (
	"fmt"
	"testing"
)

const (
	layout = "data_date=20060102"
)

func TestMod_Group(t *testing.T) {
	values := []string{
		"data_date=20221231",
		"data_date=20230120",
		"data_date=20230130",
		"data_date=20230131",
		"data_date=20230222",
		"data_date=20230230",
	}
	m1m, m1u, m1e := m1.Group(layout, values)
	fmt.Println("m1")
	fmt.Println(m1m)
	fmt.Println(m1u)
	fmt.Println(m1e)

	m2m, m2u, m2e := m2.Group(layout, values)
	fmt.Println("m2")
	fmt.Println(m2m)
	fmt.Println(m2u)
	fmt.Println(m2e)

	m3m, m3u, m3e := m3.Group(layout, values)
	fmt.Println("m3")
	fmt.Println(m3m)
	fmt.Println(m3u)
	fmt.Println(m3e)
}
