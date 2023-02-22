package util

import (
	"github.com/colinmarc/hdfs"
	"github.com/morikuni/failure"
)

func HdfsGetClient(address string) (client *hdfs.Client, err error) {
	client, err = hdfs.New(address)
	err = failure.Wrap(err)
	return
}

func HdfsListSubDirs(client *hdfs.Client, path string) (subDirs []string, err error) {
	infos, err := client.ReadDir(path)
	if err != nil {
		err = failure.Wrap(err)
		return
	}
	for _, info := range infos {
		if info.IsDir() {
			subDirs = append(subDirs, info.Name())
		}
	}
	return
}

func HdfsMove(client *hdfs.Client, src, dst string) error {
	err := client.CopyToRemote(src, dst)
	if err != nil {
		return failure.Wrap(err)
	}
	return failure.Wrap(client.Remove(src))
}
