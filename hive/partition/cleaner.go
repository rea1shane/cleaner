package main

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
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

func main() {
	// TODO 更换 yaml 路径
	file, err := ioutil.ReadFile("hive/partition/setting.yaml")
	if err != nil {
		panic(err)
	}
	var c *cleaner
	err = yaml.Unmarshal(file, &c)
	if err != nil {
		panic(err)
	}
	for _, t := range c.Policy.Mod1 {
		// TODO 获取该表的分区数组
		// TODO 用规则决定删除哪些分区
		// TODO 删除分区
	}
	// TODO Mod2 Mod3
}
