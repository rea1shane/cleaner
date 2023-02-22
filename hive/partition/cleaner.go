package main

import (
	"fmt"
	"github.com/colinmarc/hdfs"
	"github.com/rea1shane/cleaner/hive/partition/policy/mod"
	"github.com/rea1shane/cleaner/util"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"strings"
)

type cleaner struct {
	Hive struct {
		Storage struct {
			Type string `yaml:"type"`
			Hdfs struct {
				Address string `yaml:"address"`
				Path    string `yaml:"path"`
			} `yaml:"hdfs"`
			PartitionLayout string `yaml:"partition-layout"`
		} `yaml:"storage"`
	} `yaml:"hive"`
	Policy struct {
		Mod1 []string `yaml:"mod-1"`
		Mod2 []string `yaml:"mod-2"`
		Mod3 []string `yaml:"mod-3"`
	} `yaml:"policy"`
}

var (
	c                   *cleaner
	client              *hdfs.Client
	savePartitions      = make(map[string]map[string][]string)
	needCleanPartitions = make(map[string]map[string][]string)
	wrongTables         []string
	wrongPartitions     = make(map[string]map[string][]string)
)

func main() {
	// TODO 更换 yaml 路径
	file, err := ioutil.ReadFile("hive/partition/setting.yaml")
	if err != nil {
		panic(err)
	}
	err = yaml.Unmarshal(file, &c)
	if err != nil {
		panic(err)
	}

	client, err = util.GetHdfsClient(c.Hive.Storage.Hdfs.Address)
	if err != nil {
		panic(err)
	}

	for _, t := range c.Policy.Mod1 {
		groupHivePartitions(mod.M1, t)
	}
	for _, t := range c.Policy.Mod2 {
		groupHivePartitions(mod.M2, t)
	}
	for _, t := range c.Policy.Mod3 {
		groupHivePartitions(mod.M3, t)
	}

	fmt.Println(fmt.Sprintf("%+v", savePartitions))
	fmt.Println()
	fmt.Println(fmt.Sprintf("%+v", needCleanPartitions))
	fmt.Println()
	fmt.Println(fmt.Sprintf("%+v", wrongPartitions))

	// TODO 删除分区
}

func groupHivePartitions(m mod.Mod, t string) {
	if !strings.Contains(t, ".") {
		wrongTables = append(wrongTables, t)
		return
	}

	ss := strings.Split(t, ".")
	hdfsPath := c.Hive.Storage.Hdfs.Path + "/" + ss[0] + "/" + ss[1]

	partitions, _ := util.ListHdfsSubDirs(client, hdfsPath)
	matched, unmatched, errorPartitions := m.Group(c.Hive.Storage.PartitionLayout, partitions)
	if len(matched) != 0 {
		if savePartitions[ss[0]] == nil {
			savePartitions[ss[0]] = make(map[string][]string)
		}
		savePartitions[ss[0]][ss[1]] = matched
	}
	if len(unmatched) != 0 {
		if needCleanPartitions[ss[0]] == nil {
			needCleanPartitions[ss[0]] = make(map[string][]string)
		}
		needCleanPartitions[ss[0]][ss[1]] = unmatched
	}
	if len(errorPartitions) != 0 {
		if wrongPartitions[ss[0]] == nil {
			wrongPartitions[ss[0]] = make(map[string][]string)
		}
		wrongPartitions[ss[0]][ss[1]] = errorPartitions
	}
}
