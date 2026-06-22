package queue

import (
	"context"
	"fmt"

	goredis "github.com/redis/go-redis/v9"

	"github.com/rockliang/kafka-management-service/internal/kafka"
	kmsvcredis "github.com/rockliang/kafka-management-service/internal/redis"
)

// OffsetCommitter is the subset of kafka.Admin DeleteMessage needs to
// advance a (shard, partition)'s committed consumer-group offset, per
// design.md §3's low-watermark strategy.
type OffsetCommitter interface {
	CommitOffset(ctx context.Context, group, topic string, partition int32, offset int64) error
}

type DeleteMessageService struct {
	Redis     *goredis.Client
	Committer OffsetCommitter
}

// DeleteMessage acks a received message (design.md §3): the atomic ack.lua
// script removes its in-flight record + vis_index entry, removes its offset
// from the pending set, and releases any FIFO lock it held; this then
// advances the (shard, partition)'s committed watermark if doing so is safe.
// Calling DeleteMessage on an already-acked/DLQ-routed/redelivered receipt
// handle is a no-op, matching SQS's idempotent DeleteMessage semantics.
func (s *DeleteMessageService) DeleteMessage(ctx context.Context, queueName, receiptHandle string) error {
	rec, ok, err := kmsvcredis.GetInFlight(ctx, s.Redis, queueName, receiptHandle)
	if err != nil {
		return fmt.Errorf("delete message %s: %w", queueName, err)
	}
	if !ok {
		return nil
	}

	outcome, err := kmsvcredis.Ack(ctx, s.Redis, queueName, receiptHandle)
	if err != nil {
		return fmt.Errorf("delete message %s: %w", queueName, err)
	}
	if outcome != kmsvcredis.AckOutcomeAcked {
		return nil
	}

	return s.advanceWatermark(ctx, queueName, rec)
}

// advanceWatermark commits the new low-watermark for the acked message's
// (shard, partition) once the pending set has a new minimum. If nothing is
// pending (everything ever consumed on this partition has been acked), the
// last commit is left as-is rather than invented from nothing — at worst
// this means a restart replays slightly further back than strictly
// necessary, which design.md §3 accepts as the at-least-once tradeoff.
func (s *DeleteMessageService) advanceWatermark(ctx context.Context, queueName string, rec kmsvcredis.InFlightRecord) error {
	minOffset, ok, err := kmsvcredis.MinPending(ctx, s.Redis, queueName, rec.ShardID, rec.Partition)
	if err != nil {
		return fmt.Errorf("advance watermark %s: %w", queueName, err)
	}
	if !ok {
		return nil
	}
	if err := kmsvcredis.SetWatermark(ctx, s.Redis, queueName, rec.ShardID, rec.Partition, minOffset-1); err != nil {
		return fmt.Errorf("advance watermark %s: %w", queueName, err)
	}
	if s.Committer == nil {
		return nil
	}
	if err := s.Committer.CommitOffset(ctx, kafka.ConsumerGroup(queueName), rec.Topic, rec.Partition, minOffset); err != nil {
		return fmt.Errorf("advance watermark %s: %w", queueName, err)
	}
	return nil
}
