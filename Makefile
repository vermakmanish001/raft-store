# raft-store development tasks.
BINARY := raftkv
PKG    := ./...

# Dev default. The binary itself still defaults to :8080, the conventional
# choice for anyone cloning this repo; :8081 is used here only because 8080 is
# occupied on this machine. Override with: make run ADDR=:9999
ADDR   ?= :8081

.PHONY: all build run test race cover vet fmt tidy clean check smoke \
        cluster cluster-status cluster-kill-leader cluster-restart cluster-stop

all: check build

## build: compile the node binary into bin/
build:
	go build -o bin/$(BINARY) ./cmd/raftkv

## run: start a single node (override with: make run ADDR=:9999)
run:
	go run ./cmd/raftkv -addr $(ADDR)

## test: run the unit tests
test:
	go test $(PKG)

## smoke: exercise a RUNNING node over HTTP (start one first with: make run)
smoke:
	@scripts/smoke.sh http://127.0.0.1$(ADDR)

## cluster: start a local three-node cluster on :8181, :8182, :8183
cluster:
	@scripts/cluster.sh start

## cluster-status: show each node's role, term, and commit index
cluster-status:
	@scripts/cluster.sh status

## cluster-kill-leader: stop the current leader and watch the cluster recover
cluster-kill-leader:
	@scripts/cluster.sh kill-leader

## cluster-restart: kill every node and restart from the write-ahead logs
cluster-restart:
	@scripts/cluster.sh restart-all

## cluster-stop: shut the local cluster down
cluster-stop:
	@scripts/cluster.sh stop

## race: run the unit tests under the race detector
race:
	go test -race $(PKG)

## cover: produce and open a coverage report
cover:
	go test -coverprofile=coverage.out $(PKG)
	go tool cover -func=coverage.out | tail -1
	go tool cover -html=coverage.out -o coverage.html
	echo "report written to coverage.html"

## vet: run go vet
vet:
	go vet $(PKG)

## fmt: check that everything is gofmt-clean
fmt:
	test -z "$$(gofmt -l . | tee /dev/stderr)" || (echo "run: gofmt -w ." && exit 1)

## tidy: sync go.mod
tidy:
	go mod tidy

## check: the gate a commit must pass
check: fmt vet race

## clean: remove build and coverage artifacts
clean:
	rm -rf bin coverage.out coverage.html .cluster
