build:
	$(MAKE) clean
	mkdir -p bin
	go build -o bin/hive_partition_cleaner cleaner.go
	cp setting.yaml bin

package:
	$(MAKE) build
	tar zcvf hive_partition_cleaner.tar.gz -C . bin

run:
	$(MAKE) build
	bin/hive_partition_cleaner

clean:
	rm -rf bin
