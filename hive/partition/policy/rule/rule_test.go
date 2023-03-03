package rule

import (
	"fmt"
	"testing"
	"time"
)

const (
	layout = "data_date=20060102"
)

func TestTimeParse(t *testing.T) {
	var str = "data_date=20230303"
	t1me, _ := time.ParseInLocation(layout, str, time.Now().Location())
	t2me := time.Date(2023, 3, 3, 0, 0, 0, 0, time.Now().Location())
	fmt.Println(t1me)
	fmt.Println(t2me)
	fmt.Println(t1me == t2me)
}

func TestTimeAdd(t *testing.T) {
	fmt.Println(time.Now().Add(-24 * time.Hour))
}
