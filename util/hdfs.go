package util

import (
	"github.com/colinmarc/hdfs"
	"github.com/morikuni/failure"
)

func GetHdfsClient(address string) (client *hdfs.Client, err error) {
	client, err = hdfs.New(address)
	err = failure.Wrap(err)
	return
}

func ListHdfsSubDirs(client *hdfs.Client, path string) (subDirs []string, err error) {
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
