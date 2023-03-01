package storage

type Storage interface {
	ListPartitions(db, table string) (partitions []string, err error)
	BackupPartitions(db, table string, partitions []string) error
	DeletePartitions(db, table string, partitions []string) error
	Close() error
}
