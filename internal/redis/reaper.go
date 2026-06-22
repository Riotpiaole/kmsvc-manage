package redis

import (
	"context"
	_ "embed"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed lua/reap.lua
var reapScript string

//go:embed lua/ack.lua
var ackScript string

var (
	reapSHA = redis.NewScript(reapScript)
	ackSHA  = redis.NewScript(ackScript)
)

// ReapOutcome is the action taken by reap.lua for one expired in-flight entry.
type ReapOutcome string

const (
	ReapOutcomeGone      ReapOutcome = "gone"
	ReapOutcomeDLQ       ReapOutcome = "dlq"
	ReapOutcomeRedeliver ReapOutcome = "redeliver"
)

// ReapResult carries enough of the in-flight record back to the caller to
// act on it (produce to DLQ topic, or nothing further for a redeliver).
type ReapResult struct {
	Outcome      ReapOutcome
	Topic        string
	ShardID      string
	Partition    int32
	Offset       int64
	Body         string
	GroupID      string
	DedupID      string
	ReceiveCount int32
}

// Reap runs the atomic check-and-act script for one expired vis_index entry
// (design.md §5). Safe to call concurrently from multiple reaper replicas —
// only the first caller for a given receiptHandle gets a non-"gone" outcome.
func Reap(ctx context.Context, rdb *redis.Client, queue, receiptHandle string, maxReceiveCount int32) (ReapResult, error) {
	keys := []string{InFlightKey(queue, receiptHandle), VisIndexKey(queue)}
	res, err := reapSHA.Run(ctx, rdb, keys, receiptHandle, maxReceiveCount, queue).StringSlice()
	if err != nil {
		return ReapResult{}, fmt.Errorf("reap %s: %w", receiptHandle, err)
	}
	if len(res) == 0 || res[0] == string(ReapOutcomeGone) {
		return ReapResult{Outcome: ReapOutcomeGone}, nil
	}

	switch ReapOutcome(res[0]) {
	case ReapOutcomeDLQ:
		partition, _ := strconv.ParseInt(res[3], 10, 32)
		offset, _ := strconv.ParseInt(res[4], 10, 64)
		return ReapResult{
			Outcome:   ReapOutcomeDLQ,
			Topic:     res[1],
			ShardID:   res[2],
			Partition: int32(partition),
			Offset:    offset,
			Body:      res[5],
			GroupID:   res[6],
			DedupID:   res[7],
		}, nil
	case ReapOutcomeRedeliver:
		receiveCount, _ := strconv.ParseInt(res[2], 10, 32)
		return ReapResult{Outcome: ReapOutcomeRedeliver, ReceiveCount: int32(receiveCount)}, nil
	default:
		return ReapResult{}, fmt.Errorf("reap %s: unexpected outcome %q", receiptHandle, res[0])
	}
}

// AckOutcome is the result of running ack.lua.
type AckOutcome string

const (
	AckOutcomeAcked    AckOutcome = "acked"
	AckOutcomeNotFound AckOutcome = "not_found"
)

// Ack runs the atomic ack script for DeleteMessage (design.md §3).
func Ack(ctx context.Context, rdb *redis.Client, queue, receiptHandle string) (AckOutcome, error) {
	keys := []string{InFlightKey(queue, receiptHandle), VisIndexKey(queue)}
	res, err := ackSHA.Run(ctx, rdb, keys, receiptHandle, queue).StringSlice()
	if err != nil {
		return "", fmt.Errorf("ack %s: %w", receiptHandle, err)
	}
	if len(res) == 0 {
		return AckOutcomeNotFound, nil
	}
	return AckOutcome(res[0]), nil
}

// ExpiredVisIndexEntries returns up to limit receiptHandles whose visibility
// has already passed (design.md §5's sweep: ZRANGEBYSCORE ... 0 now LIMIT 0
// limit), oldest-expiry first.
func ExpiredVisIndexEntries(ctx context.Context, rdb *redis.Client, queue string, limit int64) ([]string, error) {
	handles, err := rdb.ZRangeByScore(ctx, VisIndexKey(queue), &redis.ZRangeBy{
		Min:   "0",
		Max:   strconv.FormatInt(time.Now().UnixMilli(), 10),
		Count: limit,
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("expired vis index %s: %w", queue, err)
	}
	return handles, nil
}

// PopRedeliverable pops the next receiptHandle queued for redelivery
// (design.md §4's `kmsvc:redeliver:` row), or ok=false if none is pending.
func PopRedeliverable(ctx context.Context, rdb *redis.Client, queue string) (receiptHandle string, ok bool, err error) {
	v, err := rdb.LPop(ctx, RedeliverKey(queue)).Result()
	if err == redis.Nil {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("pop redeliverable %s: %w", queue, err)
	}
	return v, true, nil
}

// PushRedeliverable re-queues a receiptHandle for redelivery. Used by
// ReceiveMessage when a popped (or freshly fetched) message can't be handed
// out yet because its FIFO group still has another message in flight — it
// goes back on the list instead of into vis_index, so a later poll retries
// it without re-fetching from Kafka.
func PushRedeliverable(ctx context.Context, rdb *redis.Client, queue, receiptHandle string) error {
	if err := rdb.RPush(ctx, RedeliverKey(queue), receiptHandle).Err(); err != nil {
		return fmt.Errorf("push redeliverable %s: %w", queue, err)
	}
	return nil
}
