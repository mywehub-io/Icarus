GOFMT := $(shell go env GOROOT)/bin/gofmt

.PHONY: test vet fmt

test:
	go test ./...

vet:
	go vet ./...

# Lists files gofmt would change and fails if there are any.
fmt:
	@test -z "$$($(GOFMT) -l .)" || { $(GOFMT) -l .; exit 1; }
