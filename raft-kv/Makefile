.PHONY: build test test-short race bench cluster chaos clean lint

build:
	go build -o bin/ ./cmd/...

test:
	go test -race -count=1 ./...

test-short:
	go test -race -short -count=1 ./...

lint:
	gofmt -l . | (! grep .) && go vet ./...

# Local 3-node cluster on ports 7000-7002 (data in ./data).
cluster: build
	./scripts/cluster.sh start

bench: build
	./bin/kvctl bench -clients 32 -duration 10s -read-ratio 0.9

# Kill/restart random replicas while a workload runs and verify no errors.
chaos: build
	./scripts/chaos.sh

clean:
	./scripts/cluster.sh stop || true
	rm -rf bin data

# ---- Raft + LSM integration (needs ../lsm-engine and a Rust toolchain)
LSM_DIR ?= ../lsm-engine
LSM_LIB := $(LSM_DIR)/target/release
lsm-lib:
	cd $(LSM_DIR) && cargo build --release -p lsm-capi
build-lsm: lsm-lib
	CGO_LDFLAGS="-L$(abspath $(LSM_LIB))" go build -tags lsm -o bin/ ./cmd/...
test-lsm: lsm-lib
	CGO_LDFLAGS="-L$(abspath $(LSM_LIB))" go test -tags lsm -race -count=1 ./kv/lsmstore ./cmd/...
chaos-lsm: build-lsm
	./bin/kvchaos -store lsm -duration $${DURATION:-30s}
.PHONY: lsm-lib build-lsm test-lsm chaos-lsm
