package main

import (
	// "context"
	"log"
	"sync"

	// "time"

	"github.com/chahatsagarmain/GoStream/api/rest"
	grpcserver "github.com/chahatsagarmain/GoStream/internal/grpc"
	"github.com/chahatsagarmain/GoStream/internal/memstore"
	"github.com/chahatsagarmain/GoStream/internal/mock"
	"github.com/chahatsagarmain/GoStream/internal/snapshot"
)

func main() {
	var wg sync.WaitGroup

	// Automatically inject our mock data so the stream doesn't boot empty
	mock.PopulateStore()

	// Handle 3 parallel systems: REST API, gRPC Server, and the Background Snapshotter
	wg.Add(3)

	go func() {
		defer wg.Done()
		rest.StartRestAPI()
		log.Println("started rest service")
	}()

	// Initialize in-memory store and use it so the package isn't an unused import.
	topics := memstore.GetTopics()
	log.Printf("memstore topics: %d", len(topics))

	// start gRPC server on :9090
	go func() {
		defer wg.Done()
		if err := grpcserver.StartGRPCServer(":9090"); err != nil {
			log.Fatalf("grpc server error: %v", err)
		}
	}()

	// start Snapshot background loop
	go func() {
		defer wg.Done()
		sp := snapshot.NewSnapShot()
		sp.StartSnapShot()
	}()

	wg.Wait()
}
