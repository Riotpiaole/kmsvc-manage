package queue

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	kmsvcredis "github.com/rockliang/kafka-management-service/internal/redis"
)

// acquireFIFOSlot claims the per-group exclusivity gate (design.md §3) so
// ReceiveMessage never hands out two in-flight messages for the same
// MessageGroupId at once. The lock's TTL matches the message's own
// visibility timeout, so a lock left by a crashed/timed-out consumer
// self-heals on the same schedule the reaper uses (design.md §5) — it is
// also released early, by ack.lua/reap.lua, on ack/redeliver/DLQ-route.
func acquireFIFOSlot(ctx context.Context, rdb *goredis.Client, queueName, groupID, receiptHandle string, visibilityTimeout time.Duration) (bool, error) {
	ok, err := kmsvcredis.AcquireFIFOLock(ctx, rdb, queueName, groupID, receiptHandle, visibilityTimeout)
	if err != nil {
		return false, fmt.Errorf("acquire fifo slot %s/%s: %w", queueName, groupID, err)
	}
	return ok, nil
}
