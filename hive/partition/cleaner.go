package main

import (
	"fmt"
	"github.com/rea1shane/cleaner/hive/partition/policy/mod"
	"github.com/rea1shane/cleaner/hive/partition/storage"
	"github.com/xuri/excelize/v2"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

type cleaner struct {
	Action struct {
		Type string `yaml:"type"`
	} `yaml:"action"`
	Hive struct {
		Storage struct {
			Type string `yaml:"type"`
			Hdfs struct {
				ConfigPath string `yaml:"config-path"`
				Username   string `yaml:"username"`
			} `yaml:"hdfs"`
			RootPath        string `yaml:"root-path"`
			PartitionLayout string `yaml:"partition-layout"`
			BackupPath      string `yaml:"backup-path"`
		} `yaml:"storage"`
	} `yaml:"hive"`
	Policy struct {
		Mod1 []string `yaml:"mod-1"`
		Mod2 []string `yaml:"mod-2"`
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
	fmt.Println("开始运行 " + time.Now().String())

	// 读取配置文件
	file, err := ioutil.ReadFile("setting.yaml")
	if err != nil {
		panic(err)
	}
	err = yaml.Unmarshal(file, &c)
	if err != nil {
		panic(err)
	}

	fmt.Println("运行模式：" + c.Action.Type)

	// 初始化存储
	switch c.Hive.Storage.Type {
	case "hdfs":
		s, err = storage.InitHdfs(
			c.Hive.Storage.Hdfs.ConfigPath,
			c.Hive.Storage.Hdfs.Username,
			c.Hive.Storage.RootPath,
			c.Hive.Storage.PartitionLayout,
			c.Hive.Storage.BackupPath,
		)
		if err != nil {
			panic(err)
		}
	}
	defer s.Close()

	// 分类分区
	for _, dbTable := range c.Policy.Mod1 {
		groupHivePartitions(mod.M1, dbTable)
	}
	for _, dbTable := range c.Policy.Mod2 {
		groupHivePartitions(mod.M2, dbTable)
	}

	saveToExcel()

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

	fmt.Println("运行结束 " + time.Now().String())
}

func groupHivePartitions(m mod.Mod, dbTable string) {
	// 记录不合规范的表名
	dbAndTable := strings.Split(dbTable, "/")
	if len(dbAndTable) != 2 {
		wrongTables = append(wrongTables, dbTable)
		return
	}

	// 获取分区列表
	partitions, err := s.ListPartitions(dbAndTable[0], dbAndTable[1])
	if err != nil {
		panic(err)
	}

	// 将分区按照规则分类
	matched, unmatched, errorPartitions := m.Group(c.Hive.Storage.PartitionLayout, partitions)
	if len(matched) != 0 {
		if savePartitions[dbAndTable[0]] == nil {
			savePartitions[dbAndTable[0]] = make(map[string][]string)
		}
		savePartitions[dbAndTable[0]][dbAndTable[1]] = matched
	}
	if len(unmatched) != 0 {
		if needCleanPartitions[dbAndTable[0]] == nil {
			needCleanPartitions[dbAndTable[0]] = make(map[string][]string)
		}
		needCleanPartitions[dbAndTable[0]][dbAndTable[1]] = unmatched
	}
	if len(errorPartitions) != 0 {
		if wrongPartitions[dbAndTable[0]] == nil {
			wrongPartitions[dbAndTable[0]] = make(map[string][]string)
		}
		wrongPartitions[dbAndTable[0]][dbAndTable[1]] = errorPartitions
	}
}

func saveToExcel() {
	fmt.Println("开始生成 Excel")

	f := excelize.NewFile()
	defer func() {
		if err := f.Close(); err != nil {
			panic(err)
		}
	}()

	if len(needCleanPartitions) != 0 {
		savePartitionsToSheet(f, "需要清理的分区", needCleanPartitions)
	}
	if len(savePartitions) != 0 {
		savePartitionsToSheet(f, "保留的分区", savePartitions)
	}
	if len(wrongPartitions) != 0 {
		savePartitionsToSheet(f, "格式错误的分区", savePartitions)
	}
	if len(wrongTables) != 0 {
		saveTablesToSheet(f, "格式错误的表", wrongTables)
	}
	f.DeleteSheet("Sheet1")

	os.MkdirAll("logs", os.FileMode(0755))
	if err := f.SaveAs("logs/" + time.Now().Format("2006-01-02") + "_" + c.Action.Type + ".xlsx"); err != nil {
		panic(err)
	}

	fmt.Println("生成 Excel 成功")
}

func savePartitionsToSheet(f *excelize.File, sheetName string, m map[string]map[string][]string) {
	// 创建 Sheet
	f.NewSheet(sheetName)
	// 行数游标
	y := 1
	for db, tableM := range m {
		// db
		dbStartY := y
		f.SetCellValue(sheetName, "A"+strconv.Itoa(y), db)
		for table, partitions := range tableM {
			// table
			tableStartY := y
			f.SetCellValue(sheetName, "B"+strconv.Itoa(y), table)
			// 转换分区为时间，并且进行正序排序
			ts := parseTimes(c.Hive.Storage.PartitionLayout, partitions)
			sort.Slice(ts, func(i, j int) bool {
				return ts[i].Sub(ts[j]) < 0
			})
			for _, t := range ts {
				// partition
				f.SetCellValue(sheetName, "C"+strconv.Itoa(y), t.Format("2006-01-02"))
				y++
			}
			f.MergeCell(sheetName, "B"+strconv.Itoa(tableStartY), "B"+strconv.Itoa(y-1))
		}
		f.MergeCell(sheetName, "A"+strconv.Itoa(dbStartY), "A"+strconv.Itoa(y-1))
	}
}

func saveTablesToSheet(f *excelize.File, sheetName string, ss []string) {
	// 创建 Sheet
	f.NewSheet(sheetName)
	// 行数游标
	y := 1
	for _, table := range ss {
		// table
		f.SetCellValue(sheetName, "A"+strconv.Itoa(y), table)
		y++
	}
}

// parseTimes 批量解析时间
func parseTimes(layout string, values []string) (ts []time.Time) {
	for _, v := range values {
		ts = append(ts, parseTime(layout, v))
	}
	return
}

// parseTime 用本地时区解析时间
// 因为这里存放的都是之前解析成功的，所以可以忽略错误
func parseTime(layout, value string) time.Time {
	t, _ := time.ParseInLocation(layout, value, time.Now().Location())
	return t
}
