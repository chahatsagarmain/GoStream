[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/chahatsagarmain/GoStream)
# GoStream

GoStream is a small, in-memory message streaming service written in Go. It provides simple topic management, publishing, and consumer offset tracking. The project exposes both a REST API and gRPC services so you can interact with it from different clients.

Key features
- Create and delete topics
- Publish messages to topics (append-only logs)
- Create consumers and track per-consumer offsets
- Fetch messages for a consumer from its current offset
- Automated background state snapshotting and disaster recovery
- REST API (Gin) and gRPC API (protobuf + grpc)

This repository is intentionally lightweight and uses an in-memory store (no external DB) by default. It's suitable for development, testing, or learning how a simple streaming/message queue works.

Contents
- `cmd/GoStream` — application entrypoint
- `api/rest` — REST handlers and router (Gin)
- `internal/grpc` — gRPC server wiring
- `internal/store` — convenience wrapper around the in-memory implementation
- `internal/memstore` — in-memory store implementation (topics, messages, offsets)
- `proto` — protobuf definitions (gostream.proto)

Quickstart — run locally

Prerequisites
- Go 1.20+ (the project uses Go modules)

Run with `go run` (development)

1. From the repo root, build & run the server:

```bash
# run the server (starts REST and gRPC services)
go run ./cmd/GoStream
```

2. By default:
- REST API starts (see `api/rest` for routes)
- gRPC server listens on `:9090`

Generate protobuf code (only needed if you edit .proto files)

If you modify `proto/gostream.proto`, regenerate Go code with `protoc` and the Go plugins:

```bash
# Install protoc (if you don't have it) and the Go plugins once
sudo apt install -y protobuf-compiler        # Debian/Ubuntu example
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Make sure the plugin binaries are on your PATH
export PATH="$PATH:$(go env GOPATH)/bin"

# Generate go files (run from repo root)
protoc \
	--go_out=. --go_opt=paths=source_relative \
	--go-grpc_out=. --go-grpc_opt=paths=source_relative \
	proto/gostream.proto

# Verify build
go build ./...
```
Notes & tips
- The default store is in-memory and ephemeral. If you restart the server you will lose topics and messages.
- The code contains an `internal/store` wrapper which delegates to the in-memory `internal/memstore`. You can add a Redis or persistent store and wire it through `internal/store` if needed.
- For IDEs: if you add/modify `.proto` files, regenerate the Go files and restart your Go language server (gopls) to pick up changes.

How it works
------------

High level flow
- Clients talk to the service via either the REST API (Gin) or the gRPC API (protobuf + gRPC).
- Both REST handlers and gRPC methods delegate to the internal `store` package. `internal/store` is a small wrapper that currently forwards to the in-memory implementation in `internal/memstore`.
- The `memstore` package holds the in-memory data structures: a list of topics, a list of consumers, segmented and evictable message logs for each topic, and per-consumer offsets.

Core data structures
- `topics`: `map[string]int` — set of topic names.
- `consumers`: `map[string]int` — set of consumer IDs.
- `topicLogs`: `map[string]*types.TopicLog` — mapping topic name to its segmented log. A topic log is split into one or more `Segment` structs:
  - `Segment` holds `Id`, `Messages` (slice of strings), `Count` (message count), `Size` (byte size), `BaseOffset` (first offset in the segment), and a `Loaded` status flag.
- `offsets`: `map[string]int` — mapping `"topic:consumer"` to the consumer's next-read offset.
- `topicConsumers`: `map[string]map[string]int` — mapping topic name -> set of subscribed consumer IDs. Enables O(1) lookup of all consumers for a given topic.

> [!NOTE]
> Topic names may **not contain `:`** since it is used as the delimiter in offset keys (`"topic:consumer"`).

Segmented Storage & Memory Eviction (Deep Dive)
-----------------------------------------------

GoStream implements a segmented storage engine designed to provide rapid in-memory operations while ensuring bounded memory usage and complete data durability. 

Here is how segmentation, sealing, eviction, and retrieval work under the hood:

### 1. Segmentation & Sealing (Append Stage)

All writes (appends) target the **active segment** (the last segment in the slice).

```
                      +-----------------------------+
                      |    Append "New Message"     |
                      +--------------+--------------+
                                     |
                                     ▼
                      +-----------------------------+
                      |   Active Segment (Id: 0)    |
                      |   - RAM: msg1, msg2         |
                      |   - Write to seg-0.log & sync|
                      |   - Size: 32B / Limit: 40B  |
                      +--------------+--------------+
                                     | (Append msg3 makes size 48B >= 40B)
                                     ▼
                      +-----------------------------+
                      |       Rotate Segment        |
                      |   - Seal active segment 0   |
                      |   - Create active segment 1 |
                      +--------------+--------------+
                                     |
                                     ▼
+-----------------------------+             +-----------------------------+
|    Sealed Segment (Id: 0)   |             |   New Active Segment (Id:1) |
|    - RAM: msg1, msg2, msg3  | <---------> |   - RAM: Empty              |
|    - Disk: seg-0.log        |             |   - Size: 0B                |
+-----------------------------+             +-----------------------------+
```

* **Trigger**: When an append makes the active segment size $\ge$ `MAX_SEGMENT_SIZE`.
* **Action**:
  1. The message is written directly to the active segment's log file `seg-<id>.log` on disk using an explicit `fsync()` system call to guarantee real-time durability.
  2. Because the active segment's size is now $\ge$ `MAX_SEGMENT_SIZE`, the active segment is **sealed** (it becomes immutable).
  3. A new active segment with `Id = active.Id + 1` and `BaseOffset = active.BaseOffset + active.Count` is initialized in RAM.

---

### 2. Size-based Retention & RAM Eviction (Memory Bounding Stage)

GoStream applies two levels of retention/memory bounding on every append:
1. **Size-based Retention (Disk Deletion)**: If the total size of all segments on disk exceeds `MAX_TOPIC_SIZE`, older segments are permanently deleted from the disk and metadata list to bound disk space usage.
2. **RAM Eviction**: To bound memory usage, if the total loaded RAM size of all segments exceeds the RAM limit (`MAX_TOPIC_SIZE / 2`, or at minimum `MAX_SEGMENT_SIZE`), older sealed segments are unloaded from RAM.

```
Total Loaded RAM Size = [Seg 0 (48B)] + [Seg 1 (48B)] = 96B  (Limit: MAX_TOPIC_SIZE/2 = 50B)
                                     │
                                     ▼
                         +───────────────────────+
                         | Evict Oldest Segments |
                         +───────────┬───────────+
                                     │
                                     ▼
+─────────────────────────────+             +─────────────────────────────+
|   Evicted Segment (Id: 0)   |             |    Sealed Segment (Id: 1)   |
|   - RAM: nil (Messages Free) | <---------> |    - RAM: msg4, msg5, msg6  |
|   - Disk: seg-0.log         |             |    - Disk: seg-1.log        |
+─────────────────────────────+             +─────────────────────────────+
```

* **Trigger**: After a segment is sealed or loaded, the system sums the sizes of all segments currently marked as `Loaded`. If this sum exceeds the RAM limit (`MAX_TOPIC_SIZE / 2`, or at minimum `MAX_SEGMENT_SIZE`), RAM eviction is triggered.
* **Action**:
  - The system iterates from the oldest segment to the second newest segment (excluding the active segment).
  - For each loaded segment, it frees the `Messages` slice (`Messages = nil`) and sets `Loaded = false`.
  - This process stops as soon as the total loaded size falls below the RAM limit.

---

### 3. Retrieval & Transparent Loading (Consume Stage)

When a consumer requests a message, the system resolves the request through a target segment based on the offset.

```
                             Consumer requests Offset: 2
                                          │
                                          ▼
                       +────────────────────────────────────+
                       |   Locate Segment containing Off:2  |
                       |   - Finds Segment 0 (Evicted)      |
                       +──────────────────┬─────────────────+
                                          │
                                          ▼
                       +────────────────────────────────────+
                       |        Load Segment 0              |
                       |   - Read DATA_DIR/topics/seg-0.log |
                       |   - Populates RAM messages         |
                       |   - Set Loaded = true              |
                       +──────────────────┬─────────────────+
                                          │
                                          ▼
                       +────────────────────────────────────+
                       |        Serve Message & Evict       |
                       |   - Return message at offset 2     |
                       |   - Run eviction if RAM limit      |
                       |     exceeded                       |
                       +────────────────────────────────────+
```

* **Trigger**: A consumer reads a message at `offset`. The system identifies which segment covers the offset (`BaseOffset <= offset < BaseOffset + Count`).
* **Action**:
  1. If the target segment is evicted (`Loaded == false`), GoStream loads it back from the disk file `seg-<id>.log` into RAM.
  2. The message is served from memory, and the consumer's offset is incremented.
  3. `evictSealedSegments` is run again. If reloading this segment pushes the total loaded size back above `MAX_TOPIC_SIZE`, the segment (or other older segments) will be evicted once more.

---
Concurrency and safety
- `memstore` uses simple RWMutexes to protect each top-level structure (topics, consumers, topicLogs, offsets). This keeps operations safe for concurrent access.
- When returning slices (e.g., `GetTopics`, `GetConsumers`) the code copies the slice header and backing data into a new slice before returning. This prevents callers from observing or mutating the store's internal backing arrays.

Typical request flow (example)
1. Create topic (REST/gRPC) -> `internal/store.CreateTopics` -> `memstore.CreateTopic` creates topic, initializes empty topic log with a single active segment.
2. Publish message (REST/gRPC) -> `internal/store.AppendToLog` -> `memstore.AppendToLog` appends to the active segment. If size exceeds `MAX_SEGMENT_SIZE`, the segment is sealed and written to disk. If total topic size exceeds `MAX_TOPIC_SIZE`, older sealed segments are evicted.
3. Create consumer (REST/gRPC) -> `internal/store.CreateConsumer` -> `memstore.CreateConsumer` adds consumer and initializes `offsets["topic:consumer"] = 0`.
4. Fetch (REST/gRPC) -> `internal/store.GetMessageFromLog` -> `memstore.GetMessageFromLog` retrieves the message from the correct segment. If that segment is not currently loaded in memory, it is read back from disk. The offset is then incremented.

Design notes
- The wrapper `internal/store` isolates higher-level handlers from the concrete storage implementation. This makes it straightforward to swap in a persistent store (e.g., Redis or a database) in the future.
- The segmented model combines the speed of in-memory messaging with the reliability of disk backups, allowing bounded RAM usage while preserving complete message histories.

### REST API Reference

**Topic Management**
- `POST /v1/topic` - Create a new topic (Payload: `{"topicname":"my-topic"}`)
- `GET /v1/topic` - List all topics
- `DELETE /v1/topic` - Delete a topic and its messages (Payload: `{"topicname":"my-topic"}`)

**Message Produce/Consume**
- `POST /v1/publish` - Publish a message to a topic (Payload: `{"topicname":"my-topic","message":"hello"}`)
- `GET /v1/message` - Fetch a message for a consumer (Query: `?topicname=my-topic&consumerid=<id>`)

**Consumer & Offset Management**
- `POST /v1/consumer` - Create a consumer assigned to a topic (Payload: `{"topicname":"my-topic"}`)
- `GET /v1/consumer` - List all consumers
- `GET /v1/consumer/topic` - List consumers subscribed to a specific topic (Query: `?topicname=my-topic`)
- `DELETE /v1/consumer` - Delete a consumer (Query: `?consumerId=<id>`)
- `GET /v1/offset` - Get a consumer's current offset (Query: `?topicname=my-topic&consumerid=<id>`)
- `GET /v1/ping` - Health check ping (produce scope)
- `GET /v1/consume/ping` - Health check ping (consume scope)

Snapshot and Recovery
---------------------
GoStream features an integrated automated background snapshot system natively configured in `internal/snapshot`. Because data volatility is a primary concern for in-memory systems, the runtime automatically serializes the entire active application state (topics, subscriber lists, current message logs, and respective consumer offsets) strictly into `.json` backup files inside a local `./snapshots` directory.

### Snapshot Mechanics
The snapshot implementation is designed to securely protect active state without corrupting concurrent memory access:
1. **Background Loop**: Firing `snapshot.NewSnapShot().StartSnapShot()` spins up a goroutine that runs infinitely. Once every 10 seconds, it triggers a system state backup.
2. **Atomic Extraction**: The system secures read locks (`RLock()`) globally across the four core data structures (`topics`, `consumers`, `messages`, `offsets`) to clone everything in a perfectly synchronized state, avoiding data race conditions from active I/O.
3. **JSON Persistence**: The unified data is serialized via `json.MarshalIndent` and safely written to local disk in a formatted `snapshot_<unix_epoch>.json` file.
4. **Boot Recovery**: Starting the application manually triggers `snapshot.RestoreSnapShot()`. It uses `os.ReadDir` to parse the `snapshots` folder, filters the latest modification timeframe, unmarshals the JSON state, and invokes an exclusive global `Lock()` across the `memstore` to instantly reinstate all live data exactly where it left off.


Benchmarks and Capacity
-----------------------
GoStream's in-memory storage includes extensive capacity benchmarks located in `internal/memstore/store_test.go`. These evaluate the system under load simulating tens of thousands of topics, published messages, and reads.

**To run the benchmarks:**
```bash
go test -bench . -benchmem ./internal/memstore
```

### Benchmark Metrics Guide

When viewing the reports below, the columns indicate the following metrics:
- **Benchmark**: The name of the benchmark function executed.
- **Iterations**: The number of times the test ran within the sampling window. Higher iterations signify faster and more stable performance.
- **Time (ns/op)**: The average duration of a single operation in nanoseconds ($1 \text{ ms} = 1,000,000 \text{ ns}$). Lower is better.
- **Memory (B/op)**: The average heap memory allocated per operation in bytes. Lower is better.
- **Allocs (allocs/op)**: The average number of heap memory allocations per operation. Zero or low allocations are optimal for garbage collector efficiency.

### Latest Benchmark Report

| Benchmark | Iterations | Time (ns/op) | Memory (B/op) | Allocs (allocs/op) |
| :--- | ---: | ---: | ---: | ---: |
| `BenchmarkCreateTopic` | 1,000,000 | 1,160 | 574 | 6 |
| `BenchmarkAppendToLog` | 10,436,515 | 196.9 | 103 | 1 |
| `BenchmarkAppendToLogParallel` | 7,607,821 | 264.3 | 108 | 1 |
| `BenchmarkGetMessageFromLog` | 3,747,411 | 307.2 | 160 | 6 |
| `BenchmarkGetMessageFromLogParallel` | 3,071,380 | 390.6 | 192 | 6 |
| `BenchmarkGetTopics` | 104,271 | 11,161 | 16,384 | 1 |
| `BenchmarkCapacity_1kTopics` | 3,182 | 363,909 | 561,122 | 5,968 |
| `BenchmarkCapacity_10kMessages` | 1,274 | 982,608 | 666,208 | 23 |
| `BenchmarkCapacity_1kConsumers` | 2,966 | 407,488 | 426,207 | 4,968 |
| `BenchmarkCapacity_MixedWorkload` | 8,562 | 196,102 | 93,750 | 1,035 |
| `BenchmarkCapacity_FullMix` | 1,946 | 605,228 | 147,968 | 6,621 |

### Storage Model Performance: In-Memory vs. Disk Swap

To illustrate the performance characteristics and cost of swapping segments to disk, we run a dedicated benchmark comparison that produces and consumes 200 messages:
1. **In-Memory**: Configured with large segment limits (`MAX_SEGMENT_SIZE = 100MB`) so that messages remain entirely in memory.
2. **Disk Swap**: Configured with tight segment limits (`MAX_SEGMENT_SIZE = 40B`, `MAX_TOPIC_SIZE = 4000B`) forcing segments to constantly seal, write to disk, evict, and reload transparently (without triggering physical retention deletion).

| Benchmark | Iterations | Time (ns/op) | Time (ms/op) | Memory (B/op) | Allocs (allocs/op) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| `BenchmarkStorage_InMemory` | 6 | 201,087,950 | 201.09 ms | 357,068 | 4,421 |
| `BenchmarkStorage_DiskSwap` | 3 | 428,842,333 | 428.84 ms | 786,778 | 6,268 |

**Key Takeaway**: 
Because GoStream uses real-time durability (flushing every single append directly to disk using `fsync`), both configurations spend the majority of their time on I/O during the append phase. However, `BenchmarkStorage_DiskSwap` requires an additional ~227.75 ms (~2.1x slower) due to the overhead of rotating and sealing segments, evicting them from RAM, and transparently reloading them from disk during the consume stage. This highlights the efficiency of caching segments in-memory when reading, while still preserving data durability on disk.


### Capacity Overview
Since upgrading the internal metadata structures from slices ($O(N)$) to constant-time hash tables (`map[string]int`), and optimizing benchmarks to avoid disk overhead, operation throughput has massively improved! 

- **Full Mixed Workload (`BenchmarkCapacity_FullMix`)**: Modeling the instantiation of 10 topics, 10 consumers per topic, 10 published messages, and 100 sequential reads per topic—now completes in collectively under **0.6ms** (605,228 ns).
- **High-Volume Publish (`BenchmarkCapacity_10kMessages`)**: Publishing 10,000 messages onto a single topic completes in under **0.98ms** (982,608 ns), showcasing highly optimized log appending performance.

Contributing
- Pull requests welcome. Create an issue or PR for larger changes.

License
- See `LICENSE` in the repository root.

If you want, I can add example curl/gRPC client snippets, dockerfile improvements, or CI steps — tell me which and I'll add them.
