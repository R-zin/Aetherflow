.PHONY: all proto test build clean lint

PROTOC_GEN_GO := $(shell which protoc-gen-go 2>/dev/null || echo $(HOME)/go/bin/protoc-gen-go)
PROTOC_GEN_GO_GRPC := $(shell which protoc-gen-go-grpc 2>/dev/null || echo $(HOME)/go/bin/protoc-gen-go-grpc)
PATH := $(PATH):$(HOME)/go/bin

proto:
	@mkdir -p api/proto/v1
	protoc --proto_path=. \
		--go_out=. --go_opt=module=github.com/R-zin/Aetherflow \
		--go-grpc_out=. --go-grpc_opt=module=github.com/R-zin/Aetherflow \
		api/proto/v1/*.proto

test:
	go test -v -race ./pkg/... ./tests/...

build:
	@mkdir -p bin
	go build -o bin/aetherd ./cmd/aetherd
	go build -o bin/aetherctl ./cmd/aetherctl
	go build -o bin/worker-example ./cmd/worker-example

clean:
	rm -rf bin/
