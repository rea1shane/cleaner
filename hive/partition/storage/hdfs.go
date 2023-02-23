package storage

import (
	"github.com/colinmarc/hdfs"
	"github.com/morikuni/failure"
)

type Hdfs struct {
	client          *hdfs.Client
	rootPath        string
	partitionLayout string
	backupPath      string
}

func InitHdfs(address, rootPath, partitionLayout, backupPath string) (*Hdfs, error) {
	client, err := hdfs.New(address)
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
		src := tablePath + "/" + partition
		dst := h.backupPath + "/" + dbName + "/" + tableName + "/" + partition
		err := h.client.CopyToRemote(src, dst)
		if err != nil {
			return failure.Wrap(err)
		}
	}
	return nil
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
