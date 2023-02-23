package storage

type Storage interface {
	ListPartitions(dbName, tableName string) (partitions []string, err error)
	BackupPartitions(dbName, tableName string, partitions []string) error
	DeletePartitions(dbName, tableName string, partitions []string) error
	Close() error
}
