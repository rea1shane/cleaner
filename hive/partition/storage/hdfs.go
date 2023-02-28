package storage

import (
	"github.com/colinmarc/hdfs"
	"github.com/morikuni/failure"
	"os"
)

type Hdfs struct {
	client          *hdfs.Client
	rootPath        string
	partitionLayout string
	backupPath      string
}

func InitHdfs(confPath, rootPath, partitionLayout, backupPath string) (*Hdfs, error) {
	hadoopConf := hdfs.LoadHadoopConf(confPath)
	namenodes, err := hadoopConf.Namenodes()
	if err != nil {
		return nil, failure.Wrap(err)
	}
	client, err := hdfs.NewClient(hdfs.ClientOptions{
		Addresses: namenodes,
	})
	if err != nil {
		return nil, failure.Wrap(err)
	}
	return &Hdfs{
		client:          client,
		rootPath:        rootPath,
		partitionLayout: partitionLayout,
		backupPath:      backupPath,
	}, nil
}

func (h *Hdfs) ListPartitions(dbName, tableName string) (partitions []string, err error) {
	infos, err := h.client.ReadDir(h.getTablePath(dbName, tableName))
	if err != nil {
		err = failure.Wrap(err)
		return
	}
	for _, info := range infos {
		if info.IsDir() {
			partitions = append(partitions, info.Name())
		}
	}
	return
}

func (h *Hdfs) BackupPartitions(dbName, tableName string, partitions []string) error {
	tablePath := h.getTablePath(dbName, tableName)
	for _, partition := range partitions {
		src := tablePath + "/" + partition + "/"
		dst := h.backupPath + "/" + dbName + "/" + tableName + "/" + partition + "/"
		// 创建目标路径的父路径
		err := h.client.MkdirAll(dst, os.FileMode(0755))
		if err != nil {
			return failure.Wrap(err)
		}
		// 遍历文件并进行移动
		files, err := h.client.ReadDir(src)
		if err != nil {
			return failure.Wrap(err)
		}
		for _, file := range files {
			err = h.client.Rename(src+file.Name(), dst+file.Name())
			if err != nil {
				return failure.Wrap(err)
			}
		}
	}
	// 删除原始路径
	return h.DeletePartitions(dbName, tableName, partitions)
}

func (h *Hdfs) DeletePartitions(dbName, tableName string, partitions []string) error {
	tablePath := h.getTablePath(dbName, tableName)
	for _, partition := range partitions {
		err := h.client.Remove(tablePath + "/" + partition)
		if err != nil {
			return failure.Wrap(err)
		}
	}
	return nil
}

func (h *Hdfs) Close() error {
	return h.client.Close()
}

func (h *Hdfs) getTablePath(dbName, tableName string) string {
	return h.rootPath + "/" + dbName + "/" + tableName
}
