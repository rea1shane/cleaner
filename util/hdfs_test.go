package util

import (
	"github.com/colinmarc/hdfs"
	"os"
	"testing"
)

var tableList = make(map[string][]string)

func TestInit(t *testing.T) {
	client, _ := hdfs.New("localhost:9000")
	for tableName, partitions := range tableList {
		for _, partition := range partitions {
			client.MkdirAll("/apps/hive/warehouse/"+tableName+"/data_date="+partition, os.FileMode(0755))
		}
	}
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
	tableList["A"] = aList
	tableList["B"] = bList
	tableList["C"] = cList
}
