package storage

import (
	"fmt"
	"github.com/colinmarc/hdfs"
	"os"
	"strings"
	"testing"
)

// 生成测试数据

var (
	client, _ = hdfs.New("localhost:9000")
	tableList = make(map[string][]string)
)

func TestLs(t *testing.T) {
	subDirs, err := client.ReadDir("/apps/hive/warehouse")
	switch err.(type) {
	case *os.PathError:
		fmt.Println("BINGO")
	default:
		for _, dir := range subDirs {
			fmt.Println(dir.Name())
		}
	}
}

func TestInit(t *testing.T) {
	for table, partitions := range tableList {
		ss := strings.Split(table, "/")
		for _, partition := range partitions {
			client.MkdirAll("/apps/hive/warehouse/"+ss[0]+"/"+ss[1]+"/data_date="+partition, os.FileMode(0755))
			client.CreateEmptyFile("/apps/hive/warehouse/" + ss[0] + "/" + ss[1] + "/data_date=" + partition + "/testFile")
		}
	}
}

func TestClean(t *testing.T) {
	client.Remove("/apps")
	client.Remove("/cleaner")
}

func init() {
	var (
		aList = []string{
			"20220915",
			"20220916",
			"20220917",
			"20220918",
			"20220919",
			"20220920",
			"20220921",
			"20220922",
			"20220923",
			"20220924",
			"20220925",
			"20220926",
			"20220927",
			"20220928",
			"20220929",
			"20220930",
			"20221001",
			"20221002",
			"20221003",
			"20221004",
			"20221005",
			"20221006",
			"20221007",
			"20221008",
			"20221009",
			"20221010",
			"20221011",
			"20221012",
			"20221013",
			"20221014",
			"20221015",
			"20221016",
			"20221017",
			"20221018",
			"20221019",
			"20221020",
			"20221021",
			"20221022",
			"20221023",
			"20221024",
			"20221025",
			"20221026",
			"20221027",
			"20221028",
			"20221029",
			"20221030",
			"20221031",
			"20221101",
			"20221102",
			"20221103",
			"20221104",
			"20221105",
			"20221106",
			"20221107",
			"20221108",
			"20221110",
			"20221111",
			"20221112",
			"20221113",
			"20221114",
			"20221115",
			"20221116",
			"20221117",
			"20221118",
			"20221119",
			"20221120",
			"20221121",
			"20221122",
			"20221123",
			"20221124",
			"20221125",
			"20221126",
			"20221127",
			"20221128",
			"20221129",
			"20221130",
			"20221231",
			"20230101",
			"20230102",
			"20230103",
			"20230104",
			"20230105",
			"20230106",
			"20230107",
			"20230108",
			"20230109",
			"20230110",
			"20230111",
			"20230112",
			"20230113",
			"20230114",
			"20230115",
			"20230116",
			"20230117",
			"20230118",
			"20230119",
			"20230120",
			"20230121",
			"20230122",
			"20230123",
			"20230124",
			"20230125",
			"20230126",
			"20230127",
			"20230128",
			"20230129",
			"20230130",
			"20230131",
			"20230201",
			"20230202",
			"20230203",
			"20230204",
			"20230205",
			"20230206",
			"20230207",
			"20230208",
			"20230209",
			"20230210",
			"20230211",
			"20230212",
			"20230213",
			"20230214",
			"20230215",
			"20230216",
			"20230217",
			"20230218",
			"20230219",
			"20230220",
			"20230221",
			"20230222",
			"20230223",
			"20230224",
			"20230225",
			"20230226",
			"20230227",
			"20230228",
			"20230301",
			"20230302",
		}
		bList = []string{
			"19990101",
			"20201221",
			"20201122",
			"20200223",
			"20211231",
			"20210831",
			"20220222",
			"20221111",
			"20230131",
		}
		cList = []string{
			"19970101",
			"20201021",
			"20200122",
			"20210223",
			"20211231",
			"20210830",
			"20220222",
			"20221111",
			"20230130",
		}
	)
	tableList["ods/A"] = aList
	tableList["dw/B"] = bList
	tableList["dw/C"] = cList
}
