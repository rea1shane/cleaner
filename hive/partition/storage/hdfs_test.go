package storage

import (
	"fmt"
	"github.com/colinmarc/hdfs"
	"os"
	"strings"
	"testing"
)

var (
	client, _ = hdfs.New("localhost:9000")
	tableList = make(map[string][]string)
)

func TestLs(t *testing.T) {
	subDirs, _ := client.ReadDir("/apps/hive/warehouse")
	for _, dir := range subDirs {
		fmt.Println(dir.Name())
	}
}

func TestInit(t *testing.T) {
	for tableName, partitions := range tableList {
		ss := strings.Split(tableName, ".")
		for _, partition := range partitions {
			client.MkdirAll("/apps/hive/warehouse/"+ss[0]+"/"+ss[1]+"/data_date="+partition, os.FileMode(0755))
			client.CreateEmptyFile("/apps/hive/warehouse/" + ss[0] + "/" + ss[1] + "/data_date=" + partition + "/testFile")
		}
	}
}

func TestClean(t *testing.T) {
	client.Remove("/apps")
}

func init() {
	var (
		aList = []string{
			"19900101",
			"20201221",
			"20201222",
			"20201223",
			"20201231",
			"20211231",
			"20220222",
			"20221111",
			"20230131",
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
	tableList["ods.A"] = aList
	tableList["dw.B"] = bList
	tableList["dw.C"] = cList
}
