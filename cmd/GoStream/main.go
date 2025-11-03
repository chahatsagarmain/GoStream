package main

import (
	// "context"
	"log"
	"sync"

	// "time"

	"github.com/chahatsagarmain/GoStream/api/rest"
	grpcserver "github.com/chahatsagarmain/GoStream/internal/grpc"
	"github.com/chahatsagarmain/GoStream/internal/memstore"
)

func main() {
	var wg sync.WaitGroup

	wg.Add(2)

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

	wg.Wait()
}
