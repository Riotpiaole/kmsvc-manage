package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// TryDedup attempts to claim a MessageDeduplicationId for a FIFO MessageGroupId
// (design.md §4 `kmsvc:dedup:` row). Returns true if this is the first send
// within the dedup window (the message should be accepted), false if it's a
// duplicate (the message should be silently deduped).
func TryDedup(ctx context.Context, rdb *redis.Client, queue, groupID, dedupID string, window time.Duration) (bool, error) {
	ok, err := rdb.SetNX(ctx, DedupKey(queue, groupID, dedupID), "1", window).Result()
	if err != nil {
		return false, fmt.Errorf("dedup check %s/%s: %w", groupID, dedupID, err)
	}
	return ok, nil
}
