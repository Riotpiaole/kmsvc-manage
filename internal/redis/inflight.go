package redis

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// InFlightRecord is the cached state of a message currently checked out by a
// consumer, per design.md §4's `kmsvc:inflight:` row.
type InFlightRecord struct {
	ShardID      string
	Topic        string
	Partition    int32
	Offset       int64
	GroupID      string
	DedupID      string
	ReceiveCount int32
	Body         string
}

// PutInFlight records a freshly received message as in-flight: writes the
// hash, sets its TTL, and adds it to the visibility-expiry index so the
// reaper (§5) can find it once visibleAt passes.
func PutInFlight(ctx context.Context, rdb *redis.Client, queue, receiptHandle string, rec InFlightRecord, visibilityTimeout time.Duration) error {
	key := InFlightKey(queue, receiptHandle)
	visibleAt := time.Now().Add(visibilityTimeout)

	pipe := rdb.TxPipeline()
	pipe.HSet(ctx, key, map[string]any{
		"shardId":      rec.ShardID,
		"topic":        rec.Topic,
		"partition":    rec.Partition,
		"offset":       rec.Offset,
		"groupId":      rec.GroupID,
		"dedupId":      rec.DedupID,
		"receiveCount": rec.ReceiveCount,
		"body":         rec.Body,
	})
	pipe.Expire(ctx, key, visibilityTimeout+time.Minute)
	pipe.ZAdd(ctx, VisIndexKey(queue), redis.Z{Score: float64(visibleAt.UnixMilli()), Member: receiptHandle})
	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("put in-flight %s: %w", receiptHandle, err)
	}
	return nil
}

// GetInFlight reads a message's in-flight record, or ok=false if it's gone
// (already acked or already DLQ-routed).
func GetInFlight(ctx context.Context, rdb *redis.Client, queue, receiptHandle string) (InFlightRecord, bool, error) {
	m, err := rdb.HGetAll(ctx, InFlightKey(queue, receiptHandle)).Result()
	if err != nil {
		return InFlightRecord{}, false, fmt.Errorf("get in-flight %s: %w", receiptHandle, err)
	}
	if len(m) == 0 {
		return InFlightRecord{}, false, nil
	}
	partition, _ := strconv.ParseInt(m["partition"], 10, 32)
	offset, _ := strconv.ParseInt(m["offset"], 10, 64)
	receiveCount, _ := strconv.ParseInt(m["receiveCount"], 10, 32)
	return InFlightRecord{
		ShardID:      m["shardId"],
		Topic:        m["topic"],
		Partition:    int32(partition),
		Offset:       offset,
		GroupID:      m["groupId"],
		DedupID:      m["dedupId"],
		ReceiveCount: int32(receiveCount),
		Body:         m["body"],
	}, true, nil
}

// ExtendVisibility implements ChangeMessageVisibility: bumps the vis_index
// score for an in-flight message without touching its receive count.
func ExtendVisibility(ctx context.Context, rdb *redis.Client, queue, receiptHandle string, newTimeout time.Duration) (bool, error) {
	exists, err := rdb.Exists(ctx, InFlightKey(queue, receiptHandle)).Result()
	if err != nil {
		return false, fmt.Errorf("extend visibility %s: %w", receiptHandle, err)
	}
	if exists == 0 {
		return false, nil
	}
	visibleAt := time.Now().Add(newTimeout)
	if err := rdb.ZAdd(ctx, VisIndexKey(queue), redis.Z{Score: float64(visibleAt.UnixMilli()), Member: receiptHandle}).Err(); err != nil {
		return false, fmt.Errorf("extend visibility %s: %w", receiptHandle, err)
	}
	return true, nil
}
