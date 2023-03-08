package storage

import (
	"fmt"
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

func InitHdfs(confPath, username, rootPath, partitionLayout, backupPath string) (*Hdfs, error) {
	hadoopConf := hdfs.LoadHadoopConf(confPath)
	namenodes, err := hadoopConf.Namenodes()
	if err != nil {
		return nil, failure.Wrap(err)
	}
	client, err := hdfs.NewClient(hdfs.ClientOptions{
		Addresses: namenodes,
		User:      username,
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

func (h *Hdfs) ListPartitions(db, table string) (partitions []string, err error) {
	infos, err := h.client.ReadDir(h.getTablePath(db, table))
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

func (h *Hdfs) BackupPartitions(db, table string, partitions []string) error {
	tablePath := h.getTablePath(db, table)
	for _, partition := range partitions {
		src := tablePath + "/" + partition + "/"
		dst := h.backupPath + "/" + db + "/" + table + "/" + partition + "/"
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
		fmt.Println("开始移动分区 " + src + " -> " + dst)
		for _, file := range files {
			err = h.client.Rename(src+file.Name(), dst+file.Name())
			if err != nil {
				return failure.Wrap(err)
			}
		}
		fmt.Println("移动分区成功 " + src + " -> " + dst)
	}
	// 删除原始路径
	return h.DeletePartitions(db, table, partitions)
}

func (h *Hdfs) DeletePartitions(db, table string, partitions []string) error {
	tablePath := h.getTablePath(db, table)
	for _, partition := range partitions {
		fmt.Println("开始删除分区 " + tablePath + "/" + partition)
		err := h.client.Remove(tablePath + "/" + partition)
		if err != nil {
			return failure.Wrap(err)
		}
		fmt.Println("删除分区成功 " + tablePath + "/" + partition)
	}
	return nil
}

func (h *Hdfs) Close() error {
	return h.client.Close()
}

func (h *Hdfs) getTablePath(db, table string) string {
	return h.rootPath + "/" + db + ".db/" + table
}
