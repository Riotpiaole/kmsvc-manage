package redis

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// AddPending records a freshly-consumed offset as not-yet-acked, per design.md
// §3/§4's `kmsvc:pending:` ZSET.
func AddPending(ctx context.Context, rdb *redis.Client, queue, shardID string, partition int32, offset int64) error {
	key := PendingKey(queue, shardID, partition)
	if err := rdb.ZAdd(ctx, key, redis.Z{Score: float64(offset), Member: offset}).Err(); err != nil {
		return fmt.Errorf("add pending %s: %w", key, err)
	}
	return nil
}

// MinPending returns the lowest not-yet-acked offset for a (shard, partition),
// or ok=false if nothing is pending — i.e. the watermark can advance to the
// last consumed offset.
func MinPending(ctx context.Context, rdb *redis.Client, queue, shardID string, partition int32) (offset int64, ok bool, err error) {
	res, err := rdb.ZRangeWithScores(ctx, PendingKey(queue, shardID, partition), 0, 0).Result()
	if err != nil {
		return 0, false, fmt.Errorf("min pending %s: %w", PendingKey(queue, shardID, partition), err)
	}
	if len(res) == 0 {
		return 0, false, nil
	}
	return int64(res[0].Score), true, nil
}

// SetWatermark stores the committable watermark for a (shard, partition),
// per design.md §3's background committer.
func SetWatermark(ctx context.Context, rdb *redis.Client, queue, shardID string, partition int32, offset int64) error {
	key := WatermarkKey(queue, shardID, partition)
	if err := rdb.Set(ctx, key, offset, 0).Err(); err != nil {
		return fmt.Errorf("set watermark %s: %w", key, err)
	}
	return nil
}

// GetWatermark returns the last-committed watermark for a (shard, partition),
// or ok=false if none has been committed yet.
func GetWatermark(ctx context.Context, rdb *redis.Client, queue, shardID string, partition int32) (offset int64, ok bool, err error) {
	v, err := rdb.Get(ctx, WatermarkKey(queue, shardID, partition)).Result()
	if err == redis.Nil {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("get watermark %s: %w", WatermarkKey(queue, shardID, partition), err)
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("parse watermark %s: %w", WatermarkKey(queue, shardID, partition), err)
	}
	return n, true, nil
}

// AcquireFIFOLock claims the per-group exclusivity gate (design.md §3/§4's
// `kmsvc:fifo_lock:` row) so only one in-flight message per MessageGroupId
// is ever handed out.
func AcquireFIFOLock(ctx context.Context, rdb *redis.Client, queue, groupID, receiptHandle string, ttl time.Duration) (bool, error) {
	ok, err := rdb.SetNX(ctx, FIFOLockKey(queue, groupID), receiptHandle, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("acquire fifo lock %s: %w", groupID, err)
	}
	return ok, nil
}
