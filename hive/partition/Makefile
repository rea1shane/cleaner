build:
	$(MAKE) clean
	mkdir -p bin
	go build -o bin/hive_partition_cleaner cleaner.go
	cp setting.yaml bin

package:
	$(MAKE) build
	cp -R bin hive_partition_cleaner
	cp README.md hive_partition_cleaner
	tar zcvf hive_partition_cleaner.tar.gz -C . hive_partition_cleaner
	rm -rf hive_partition_cleaner

run:
	$(MAKE) build
	bin/hive_partition_cleaner

clean:
	rm -rf bin
	rm -rf logs
	rm -rf hive_partition_cleaner.tar.gz
