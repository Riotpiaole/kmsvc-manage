// Package queue implements the message-plane business logic from
// design.md §2b/§3: SendMessage/ReceiveMessage/DeleteMessage/
// ChangeMessageVisibility, FIFO gating, and shard-aware routing.
package queue

import (
	"context"
	"fmt"

	goredis "github.com/redis/go-redis/v9"

	"github.com/rockliang/kafka-management-service/internal/kafka"
	kmsvcredis "github.com/rockliang/kafka-management-service/internal/redis"
)

// ShardRouter resolves a routing key to a shard by reading the cached shard
// map the queue-operator writes (design.md §4 `kmsvc:shardmap:`). It is the
// only piece of core logic that needs to know shards exist — everything
// downstream operates against a resolved (shard, topic) pair exactly as it
// would against a single topic.
//
// In-process caching of the shard map (design.md §4's stated optimization)
// is intentionally not implemented in v1: a direct Redis read per call is
// simpler and still cheap relative to a Kafka round trip, and can be added
// later without changing this type's interface.
type ShardRouter struct {
	Redis *goredis.Client
}

// RouteForSend returns the Active shard a new message with the given
// routing key (MessageGroupId for FIFO, a random UUID for standard queues)
// should be produced to.
func (r *ShardRouter) RouteForSend(ctx context.Context, queueName, routingKey string) (kafka.Shard, error) {
	shards, err := r.shards(ctx, queueName)
	if err != nil {
		return kafka.Shard{}, err
	}
	shard, found := kafka.SelectShard(kafka.ActiveShards(shards), routingKey)
	if !found {
		return kafka.Shard{}, fmt.Errorf("no active shard covers the routing key for queue %s", queueName)
	}
	return shard, nil
}

// ConsumableShards returns every shard a consumer should be subscribed to:
// Active (write+read) and Closing (read-only, draining) — everything except
// Closed, whose topic has already been deleted.
func (r *ShardRouter) ConsumableShards(ctx context.Context, queueName string) ([]kafka.Shard, error) {
	shards, err := r.shards(ctx, queueName)
	if err != nil {
		return nil, err
	}
	out := make([]kafka.Shard, 0, len(shards))
	for _, s := range shards {
		if s.Phase != "Closed" {
			out = append(out, s)
		}
	}
	return out, nil
}

func (r *ShardRouter) shards(ctx context.Context, queueName string) ([]kafka.Shard, error) {
	shards, ok, err := kmsvcredis.GetShardMap(ctx, r.Redis, queueName)
	if err != nil {
		return nil, fmt.Errorf("read shard map %s: %w", queueName, err)
	}
	if !ok || len(shards) == 0 {
		return nil, fmt.Errorf("queue %s has no shard map yet (not reconciled)", queueName)
	}
	return shards, nil
}
