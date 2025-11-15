[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/chahatsagarmain/GoStream)
# GoStream

GoStream is a small, in-memory message streaming service written in Go. It provides simple topic management, publishing, and consumer offset tracking. The project exposes both a REST API and gRPC services so you can interact with it from different clients.

Key features
- Create and delete topics
- Publish messages to topics (append-only logs)
- Create consumers and track per-consumer offsets
- Fetch messages for a consumer from its current offset
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

Docker Compose

This repo includes a `compose.yaml` to run the service inside a container for quick testing.

Build and run with Docker Compose:

```bash
# Build the service image and start with docker-compose
docker compose up --build

# Or (legacy) with docker-compose:
docker-compose up --build
```

After compose finishes starting, the same endpoints are available (gRPC port and REST port depending on the compose settings).

Notes & tips
- The default store is in-memory and ephemeral. If you restart the server you will lose topics and messages.
- The code contains an `internal/store` wrapper which delegates to the in-memory `internal/memstore`. You can add a Redis or persistent store and wire it through `internal/store` if needed.
- For IDEs: if you add/modify `.proto` files, regenerate the Go files and restart your Go language server (gopls) to pick up changes.

How it works
------------

High level flow
- Clients talk to the service via either the REST API (Gin) or the gRPC API (protobuf + gRPC).
- Both REST handlers and gRPC methods delegate to the internal `store` package. `internal/store` is a small wrapper that currently forwards to the in-memory implementation in `internal/memstore`.
- The `memstore` package holds the in-memory data structures: a list of topics, a list of consumers, per-topic append-only message logs, and per-consumer offsets for each topic.

Core data structures
- topics: []string — list of topic names.
- consumers: []string — list of consumer IDs.
- messages: map[string][]string — mapping topic name -> slice of messages (append-only log).
- offsets: map[string]int — mapping `"topic:consumer"` to the consumer's next-read offset.

Concurrency and safety
- `memstore` uses simple RWMutexes to protect each top-level structure (topics, consumers, messages, offsets). This keeps operations safe for concurrent access.
- When returning slices (e.g., `GetTopics`, `GetConsumers`) the code copies the slice header and backing data into a new slice before returning. This prevents callers from observing or mutating the store's internal backing arrays.

Typical request flow (example)
1. Create topic (REST/gRPC) -> `internal/store.CreateTopics` -> `memstore.CreateTopic` creates topic, initializes empty message log.
2. Publish message (REST/gRPC) -> `internal/store.AppendToLog` -> `memstore.AppendToLog` appends to `messages[topic]`.
3. Create consumer (REST/gRPC) -> `internal/store.CreateConsumer` -> `memstore.CreateConsumer` adds consumer and initializes `offsets["topic:consumer"] = 0`.
4. Fetch (REST/gRPC) -> `internal/store.GetMessageFromLog` -> `memstore.GetMessageFromLog` reads `messages[topic][offset]`, increments offset with `memstore.SetOffset`.

Design notes
- The wrapper `internal/store` isolates higher-level handlers from the concrete storage implementation. This makes it straightforward to swap in a persistent store (e.g., Redis or a database) in the future.
- The in-memory approach is simple and fast for development, but not durable. For production you'd wire a persistent backend and rework some concurrency/atomicity details.

Example requests
- Create topic (REST): POST /v1/produce/topic {"topicname":"my-topic"}
- Publish message (REST): POST /v1/produce/message {"topicname":"my-topic","message":"hello"}
- Create consumer (REST): POST /v1/consume/consumer {"topicname":"my-topic"}
- Fetch (REST): GET /v1/produce/message?topicname=my-topic&consumerid=<id>

Contributing
- Pull requests welcome. Create an issue or PR for larger changes.

License
- See `LICENSE` in the repository root.

If you want, I can add example curl/gRPC client snippets, dockerfile improvements, or CI steps — tell me which and I'll add them.
