package main

import (
	// "context"
	"log"
	"sync"
	// "time"

	"github.com/chahatsagarmain/GoStream/api/rest"
	"github.com/chahatsagarmain/GoStream/internal/redisstore"
)

func main() {
	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		defer wg.Done()
		rest.StartRestAPI()
		log.Println("started rest service")
	}()

	go func() {
		defer wg.Done()
		// ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		// defer cancel()

		err := redisstore.InitRedisClient()
		if err != nil {
			log.Fatalf("redis ping fatal error : %s ", err)
		}
		log.Printf("redis connected")
	}()

	wg.Wait()
}
