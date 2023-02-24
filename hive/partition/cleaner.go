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
	Action struct {
		Type string `yaml:"type"`
	} `yaml:"action"`
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
	wrongTables         []string
	savePartitions      = make(map[string]map[string][]string)
	needCleanPartitions = make(map[string]map[string][]string)
	wrongPartitions     = make(map[string]map[string][]string)
)

func main() {
	file, err := ioutil.ReadFile("setting.yaml")
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

	fmt.Println("格式错误的表名：")
	fmt.Println(wrongTables)
	fmt.Println("保留的分区：")
	fmt.Println(savePartitions)
	fmt.Println("需要清理的分区：")
	fmt.Println(needCleanPartitions)
	fmt.Println("格式错误的分区：")
	fmt.Println(wrongPartitions)

	for db, m := range needCleanPartitions {
		for table, partitions := range m {
			var err error
			switch c.Action.Type {
			case "backup":
				err = s.BackupPartitions(db, table, partitions)
			case "delete":
				err = s.DeletePartitions(db, table, partitions)
			}
			if err != nil {
				panic(err)
			}
		}
	}
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
