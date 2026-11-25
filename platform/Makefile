# Regenerates the Go code for the vendored protos. Needs protoc and protoc-gen-go on PATH; the
# well-known types (google/protobuf/*.proto) come from protoc's own include directory.

PROTOC ?= protoc
MODULE := github.com/wehubfusion/Icarus/platform
GOFMT := $(shell go env GOROOT)/bin/gofmt

.PHONY: proto test vet fmt

proto:
	$(PROTOC) -I proto \
		--go_out=. --go_opt=module=$(MODULE) \
		proto/audit/v1/audit.proto \
		proto/iriswire/v1/iriswire.proto

test:
	go test ./...

vet:
	go vet ./...

# Lists files gofmt would change and fails if there are any.
fmt:
	@test -z "$$($(GOFMT) -l .)" || { $(GOFMT) -l .; exit 1; }
