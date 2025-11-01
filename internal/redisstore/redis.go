package redisstore

import (

	"github.com/redis/go-redis/v9"
)

var Client *redis.Client

func InitRedisClient() *redis.Client {
	rdb := redis.NewClient(
		&redis.Options{
			Addr: "localhost:6379",
			Password: "",
			DB: 0,
			Protocol: 2,
		},
	)

	Client = rdb

	return rdb
}




