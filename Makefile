.PHONY : proto test build clean autocomplete-bash

OUTPUT_DIR=${PWD}/dist
BINARY_NAME_LINUX=gokv-linux-amd64
BINARY_PATH_LINUX=${OUTPUT_DIR}/${BINARY_NAME_LINUX}
AUTOCOMPLETE_PATH_BASH=${OUTPUT_DIR}/autocomplete/bash/gokv

all: test proto build autocomplete-bash

proto:
	@protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative api/gokv.proto

test:
	go test -timeout 30s -v ./...

build: proto
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o ${BINARY_PATH_LINUX}
	@echo "built ${BINARY_PATH_LINUX}"

clean:
	-rm -f ${BINARY_PATH_LINUX}

autocomplete-bash:
	@mkdir -p ${OUTPUT_DIR}/autocomplete/bash
	@${BINARY_PATH_LINUX} completion bash > ${AUTOCOMPLETE_PATH_BASH}
