package redis

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// QueueMeta is the queue-operator-written config snapshot a message-plane
// replica reads to know how to serve a queue, per design.md §4's `kmsvc:queue:` row.
type QueueMeta struct {
	FIFO                     bool
	VisibilityTimeoutSeconds int32
	MaxReceiveCount          int32
	DLQQueueName             string
	PartitionsPerShard       int32
	RetentionSeconds         int32
	CreatedAt                time.Time
}

// PutQueueMeta writes a queue's config and publishes an invalidation so any
// in-process caches refresh on next read.
func PutQueueMeta(ctx context.Context, rdb *redis.Client, queue string, m QueueMeta) error {
	key := QueueMetaKey(queue)
	createdAt := m.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now()
	}
	pipe := rdb.TxPipeline()
	pipe.HSet(ctx, key, map[string]any{
		"fifo":                     m.FIFO,
		"visibilityTimeoutSeconds": m.VisibilityTimeoutSeconds,
		"maxReceiveCount":          m.MaxReceiveCount,
		"dlqQueueName":             m.DLQQueueName,
		"partitionsPerShard":       m.PartitionsPerShard,
		"retentionSeconds":         m.RetentionSeconds,
		"createdAt":                createdAt.Unix(),
	})
	pipe.Publish(ctx, QueueMetaChannel(queue), "invalidate")
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("put queue meta %s: %w", queue, err)
	}
	return nil
}

// GetQueueMeta reads a queue's config, or ok=false if it doesn't exist.
func GetQueueMeta(ctx context.Context, rdb *redis.Client, queue string) (QueueMeta, bool, error) {
	m, err := rdb.HGetAll(ctx, QueueMetaKey(queue)).Result()
	if err != nil {
		return QueueMeta{}, false, fmt.Errorf("get queue meta %s: %w", queue, err)
	}
	if len(m) == 0 {
		return QueueMeta{}, false, nil
	}
	visTimeout, _ := strconv.ParseInt(m["visibilityTimeoutSeconds"], 10, 32)
	maxReceive, _ := strconv.ParseInt(m["maxReceiveCount"], 10, 32)
	partitionsPerShard, _ := strconv.ParseInt(m["partitionsPerShard"], 10, 32)
	retention, _ := strconv.ParseInt(m["retentionSeconds"], 10, 32)
	createdAtUnix, _ := strconv.ParseInt(m["createdAt"], 10, 64)
	return QueueMeta{
		FIFO:                     m["fifo"] == "1",
		VisibilityTimeoutSeconds: int32(visTimeout),
		MaxReceiveCount:          int32(maxReceive),
		DLQQueueName:             m["dlqQueueName"],
		PartitionsPerShard:       int32(partitionsPerShard),
		RetentionSeconds:         int32(retention),
		CreatedAt:                time.Unix(createdAtUnix, 0),
	}, true, nil
}

// DeleteQueueMeta removes a queue's config, used by the queue-operator on
// Queue deletion.
func DeleteQueueMeta(ctx context.Context, rdb *redis.Client, queue string) error {
	if err := rdb.Del(ctx, QueueMetaKey(queue)).Err(); err != nil {
		return fmt.Errorf("delete queue meta %s: %w", queue, err)
	}
	return nil
}
