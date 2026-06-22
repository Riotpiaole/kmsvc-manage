package redis

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/rockliang/kafka-management-service/internal/kafka"
)

// PutShardMap writes the active shard set for a queue (design.md §4's
// `kmsvc:shardmap:` row) and publishes an invalidation so cached readers
// refresh. Written by the queue-operator on every reconcile that changes
// shard membership (initial create, split, close).
func PutShardMap(ctx context.Context, rdb *redis.Client, queue string, shards []kafka.Shard) error {
	data, err := json.Marshal(shards)
	if err != nil {
		return fmt.Errorf("marshal shard map %s: %w", queue, err)
	}
	pipe := rdb.TxPipeline()
	pipe.Set(ctx, ShardMapKey(queue), data, 0)
	pipe.Publish(ctx, ShardMapChannel(queue), "invalidate")
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("put shard map %s: %w", queue, err)
	}
	return nil
}

// GetShardMap reads the active shard set for a queue, or ok=false if the
// queue has no shard map yet (not yet reconciled).
func GetShardMap(ctx context.Context, rdb *redis.Client, queue string) ([]kafka.Shard, bool, error) {
	data, err := rdb.Get(ctx, ShardMapKey(queue)).Bytes()
	if err == redis.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("get shard map %s: %w", queue, err)
	}
	var shards []kafka.Shard
	if err := json.Unmarshal(data, &shards); err != nil {
		return nil, false, fmt.Errorf("unmarshal shard map %s: %w", queue, err)
	}
	return shards, true, nil
}

// DeleteShardMap removes a queue's shard map, used by the queue-operator on
// Queue deletion.
func DeleteShardMap(ctx context.Context, rdb *redis.Client, queue string) error {
	if err := rdb.Del(ctx, ShardMapKey(queue)).Err(); err != nil {
		return fmt.Errorf("delete shard map %s: %w", queue, err)
	}
	return nil
}
