package main

import (
	"fmt"
	"github.com/rea1shane/cleaner/hive/partition/policy/mod"
	"github.com/rea1shane/cleaner/hive/partition/storage"
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
			} `yaml:"hdfs"`
			RootPath        string `yaml:"root-path"`
			PartitionLayout string `yaml:"partition-layout"`
			BackupPath      string `yaml:"backup-path"`
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
	s                   storage.Storage
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

	switch c.Hive.Storage.Type {
	case "hdfs":
		s, err = storage.InitHdfs(
			c.Hive.Storage.Hdfs.Address,
			c.Hive.Storage.RootPath,
			c.Hive.Storage.PartitionLayout,
			c.Hive.Storage.BackupPath,
		)
		if err != nil {
			panic(err)
		}
	}
	defer s.Close()

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

	// 获取分区列表
	partitions, err := s.ListPartitions(dat[0], dat[1])
	if err != nil {
		panic(err)
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
