.PHONY: build clean run

GITHASH := $(shell git rev-parse --short HEAD)
GOLDFLAGS += -X app.__GITHASH__=$(GITHASH)
GOFLAGS = -ldflags "$(GOLDFLAGS)"

all: build-dev
third: 
	rm -rf ./lib 
	mkdir -p ./lib
	cd cpp && g++ -fpic -shared -o libdisgorge.so disgorge.cpp -l rocksdb -std=c++17 && cp libdisgorge.so ../lib
build: third
	go build -o build/disgorge $(GOFLAGS)
build-dev: build
	cp -rf conf/dev build/conf
build-prod: build
	cp -rf conf/prod build/conf
run: build-dev
	./build/disgorge -config="./build/conf"
clean:
	rm -rf ./build