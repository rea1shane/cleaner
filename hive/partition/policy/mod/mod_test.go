package mod

import (
	"fmt"
	"github.com/rea1shane/cleaner/hive/partition/policy/rule"
	"testing"
)

const (
	layout = "data_date=20060102"
)

func TestMod_Group(t *testing.T) {
	var rules []rule.Rule
	r1 := rule.R1{}
	rules = append(rules, r1)
	m1 := Mod{rules: rules}
	values := []string{
		"data_date=20221231",
		"data_date=20230120",
		"data_date=20230130",
		"data_date=20230131",
		"data_date=20230222",
		"data_date=20230230",
	}
	matched, unmatched, errorValue := m1.Group(layout, values)
	fmt.Println(matched)
	fmt.Println(unmatched)
	fmt.Println(errorValue)
}
