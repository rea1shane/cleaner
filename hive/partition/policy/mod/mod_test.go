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
		"data_date=20201230",
		"data_date=20221231",
		"data_date=20230120",
		"data_date=20230130",
		"data_date=20230131",
		"data_date=20230222",
		"data_date=20230229",
	}
	m1m, m1u, m1e := M1.Group(layout, values)
	fmt.Println("M1")
	fmt.Println(m1m)
	fmt.Println(m1u)
	fmt.Println(m1e)

	m2m, m2u, m2e := M2.Group(layout, values)
	fmt.Println("M2")
	fmt.Println(m2m)
	fmt.Println(m2u)
	fmt.Println(m2e)
}
