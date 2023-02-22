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

	client, err = util.HdfsGetClient(c.Hive.Storage.Hdfs.Address)
	if err != nil {
		panic(err)
	}
	defer client.Close()

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
	// 记录不合规范的表名
	dat := strings.Split(t, ".")
	if len(dat) != 2 {
		wrongTables = append(wrongTables, t)
		return
	}

	var (
		path       string
		partitions []string
	)

	// 获取存储路径和分区
	switch c.Hive.Storage.Type {
	case "hdfs":
		path = getHdfsPath(dat[0], dat[1])
		partitions, _ = util.HdfsListSubDirs(client, path)
	}

	// 将分区按照规则分类
	matched, unmatched, errorPartitions := m.Group(c.Hive.Storage.PartitionLayout, partitions)
	if len(matched) != 0 {
		if savePartitions[dat[0]] == nil {
			savePartitions[dat[0]] = make(map[string][]string)
		}
		savePartitions[dat[0]][dat[1]] = matched
	}
	if len(unmatched) != 0 {
		if needCleanPartitions[dat[0]] == nil {
			needCleanPartitions[dat[0]] = make(map[string][]string)
		}
		needCleanPartitions[dat[0]][dat[1]] = unmatched
	}
	if len(errorPartitions) != 0 {
		if wrongPartitions[dat[0]] == nil {
			wrongPartitions[dat[0]] = make(map[string][]string)
		}
		wrongPartitions[dat[0]][dat[1]] = errorPartitions
	}
}

func getHdfsPath(dbName, tableName string) string {
	return c.Hive.Storage.Hdfs.Path + "/" + dbName + "/" + tableName
}
