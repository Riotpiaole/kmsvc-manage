package redis

import "github.com/redis/go-redis/v9"

// NewClient builds a Redis client for the given address/password/db.
func NewClient(addr, password string, db int) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})
}
