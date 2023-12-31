.PHONY : proto test build clean autocomplete-bash

OUTPUT_DIR=${PWD}/dist
BINARY_DIR=${OUTPUT_DIR}/bin
BINARY_NAME=keyquarry
BINARY_PATH=${BINARY_DIR}/${BINARY_NAME}
AUTOCOMPLETE_PATH_BASH=${OUTPUT_DIR}/autocomplete/bash/keyquarry


BUILD_TIME=$(shell date -u --iso-8601=seconds)
BUILD_USER=$(shell whoami)
COMMIT := $(if $(and $(wildcard .git),$(shell which git)),$(shell git rev-parse HEAD), "unknown")
VERSION ?= unknown
CGO_ENABLED ?= 0
GOOS ?= linux
GOARCH ?= amd64

GO_BUILD_LDFLAGS=-ldflags "-X 'github.com/arcward/keyquarry/build.Version=${VERSION}' -X 'github.com/arcward/keyquarry/build.Time=${BUILD_TIME}' -X 'github.com/arcward/keyquarry/build.User=${BUILD_USER}' -X 'github.com/arcward/keyquarry/build.Commit=${COMMIT}'"
GO_BUILD_FLAGS=${GO_BUILD_LDFLAGS} -o ${BINARY_PATH}

.PHONY: all
all: test proto build autocomplete-bash

.PHONY: proto
proto:
	@protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative api/keyquarry.proto


.PHONY: test
test:
	go test -timeout 30s -v ./...

.PHONY: build
build: proto
	@echo "Building..."
	GOOS=${GOOS} GOARCH=${GOARCH} CGO_ENABLED=${CGO_ENABLED} go build ${GO_BUILD_FLAGS}
	@echo "built ${BINARY_PATH} (commit: ${COMMIT}) (time: ${BUILD_TIME}) (user: ${BUILD_USER}) (GOOS: ${GOOS} GOARCH: ${GOARCH} CGO_ENABLED: ${CGO_ENABLED})"

.PHONY: clean
clean:
	-go clean
	-rm -rf ${OUTPUT_DIR}

.PHONY: autocomplete-bash
autocomplete-bash:
	@mkdir -p ${OUTPUT_DIR}/autocomplete/bash
	@${BINARY_PATH} completion bash > ${AUTOCOMPLETE_PATH_BASH}
