package redisstore

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	Client *redis.Client
	once   sync.Once
)

// InitRedisClient initializes the package-level Redis client. It reads
// REDIS_ADDR from environment (falls back to localhost:6379). Returns
// an error if the initial ping fails.
func InitRedisClient() error {
	var initErr error
	once.Do(func() {
		addr := os.Getenv("REDIS_ADDR")
		if addr == "" {
			addr = "localhost:6379"
		}
		fmt.Printf("%v", addr)
		rdb := redis.NewClient(&redis.Options{
			Addr:     addr,
			Password: "",
			DB:       0,
			Protocol: 2,
		})

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		if err := rdb.Ping(ctx).Err(); err != nil {
			initErr = err
			return
		}

		Client = rdb
	})

	return initErr
}
