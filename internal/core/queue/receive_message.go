package queue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"

	"github.com/rockliang/kafka-management-service/internal/kafka"
	kmsvcredis "github.com/rockliang/kafka-management-service/internal/redis"
)

const defaultPollInterval = 200 * time.Millisecond // design.md §2b

// Fetcher is the subset of kafka.Consumer ReceiveMessage needs: one
// non-blocking poll iteration across whatever shard topics it's currently
// subscribed to (design.md §6 — every Active+Closing shard).
type Fetcher interface {
	Poll(ctx context.Context) ([]kafka.Record, error)
}

// Message is one message handed back to a ReceiveMessage caller.
type Message struct {
	ReceiptHandle string
	Body          string
	ReceiveCount  int32
}

type ReceiveMessageInput struct {
	QueueName                 string
	MaxNumberOfMessages       int32
	WaitTime                  time.Duration
	VisibilityTimeoutOverride time.Duration // 0 = use the queue's configured default
}

type ReceiveMessageService struct {
	Redis   *goredis.Client
	Fetcher Fetcher
	Router  *ShardRouter

	// PollInterval is the short sleep between Kafka+Redis poll iterations
	// during a long-poll wait (design.md §2b); defaults to 200ms.
	PollInterval time.Duration
}

// ReceiveMessage implements ReceiveMessage's SQS-style long polling
// (design.md §2b): pops anything already queued for redelivery first
// (design.md §4, avoids re-fetching by offset), then polls Kafka for new
// records, looping until max_number_of_messages is satisfied or wait_time
// elapses.
func (s *ReceiveMessageService) ReceiveMessage(ctx context.Context, in ReceiveMessageInput) ([]Message, error) {
	meta, ok, err := kmsvcredis.GetQueueMeta(ctx, s.Redis, in.QueueName)
	if err != nil {
		return nil, fmt.Errorf("receive message %s: %w", in.QueueName, err)
	}
	if !ok {
		return nil, fmt.Errorf("queue %s not found", in.QueueName)
	}

	visTimeout := in.VisibilityTimeoutOverride
	if visTimeout <= 0 {
		visTimeout = time.Duration(meta.VisibilityTimeoutSeconds) * time.Second
	}
	max := in.MaxNumberOfMessages
	if max <= 0 || max > 10 {
		max = 10
	}
	interval := s.PollInterval
	if interval <= 0 {
		interval = defaultPollInterval
	}

	shards, err := s.Router.ConsumableShards(ctx, in.QueueName)
	if err != nil {
		return nil, err
	}
	topicToShardID := make(map[string]string, len(shards))
	for _, sh := range shards {
		topicToShardID[sh.Topic] = sh.ID
	}

	deadline := time.Now().Add(in.WaitTime)
	out := make([]Message, 0, max)
	for {
		for int32(len(out)) < max {
			msg, handed, err := s.handOutRedeliverable(ctx, meta, in.QueueName, visTimeout)
			if err != nil {
				return nil, err
			}
			if !handed {
				break
			}
			out = append(out, msg)
		}

		if int32(len(out)) < max {
			// kgo's PollFetches blocks until either records are available or
			// its context is done, so each iteration gets its own
			// short-lived context bounded by the poll interval — otherwise a
			// quiet topic would block the very first iteration for the
			// entire wait_time instead of retrying every interval.
			pollCtx, cancel := context.WithTimeout(ctx, interval)
			records, err := s.Fetcher.Poll(pollCtx)
			cancel()
			if err != nil && ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if err != nil && !errors.Is(err, context.DeadlineExceeded) {
				return nil, fmt.Errorf("receive message %s: %w", in.QueueName, err)
			}
			for _, rec := range records {
				if int32(len(out)) >= max {
					break
				}
				shardID, known := topicToShardID[rec.Topic]
				if !known {
					continue // topic belongs to a shard that's since closed
				}
				msg, handed, err := s.handOutFresh(ctx, meta, in.QueueName, shardID, rec, visTimeout)
				if err != nil {
					return nil, err
				}
				if handed {
					out = append(out, msg)
				}
			}
		}

		if len(out) > 0 || time.Now().After(deadline) {
			return out, nil
		}
		select {
		case <-ctx.Done():
			return out, ctx.Err()
		case <-time.After(interval):
		}
	}
}

func (s *ReceiveMessageService) handOutRedeliverable(ctx context.Context, meta kmsvcredis.QueueMeta, queueName string, visTimeout time.Duration) (Message, bool, error) {
	handle, ok, err := kmsvcredis.PopRedeliverable(ctx, s.Redis, queueName)
	if err != nil {
		return Message{}, false, fmt.Errorf("receive message %s: %w", queueName, err)
	}
	if !ok {
		return Message{}, false, nil
	}

	rec, ok, err := kmsvcredis.GetInFlight(ctx, s.Redis, queueName, handle)
	if err != nil {
		return Message{}, false, fmt.Errorf("receive message %s: %w", queueName, err)
	}
	if !ok {
		// Acked/DLQ-routed between the push onto the redeliver list and this
		// pop — nothing to hand out, caller should keep draining the list.
		return Message{}, false, nil
	}

	if rec.GroupID != "" {
		acquired, err := acquireFIFOSlot(ctx, s.Redis, queueName, rec.GroupID, handle, visTimeout)
		if err != nil {
			return Message{}, false, err
		}
		if !acquired {
			if err := kmsvcredis.PushRedeliverable(ctx, s.Redis, queueName, handle); err != nil {
				return Message{}, false, fmt.Errorf("receive message %s: %w", queueName, err)
			}
			return Message{}, false, nil
		}
	}

	if _, err := kmsvcredis.ExtendVisibility(ctx, s.Redis, queueName, handle, visTimeout); err != nil {
		return Message{}, false, fmt.Errorf("receive message %s: %w", queueName, err)
	}
	return Message{ReceiptHandle: handle, Body: rec.Body, ReceiveCount: rec.ReceiveCount}, true, nil
}

func (s *ReceiveMessageService) handOutFresh(ctx context.Context, meta kmsvcredis.QueueMeta, queueName, shardID string, rec kafka.Record, visTimeout time.Duration) (Message, bool, error) {
	if err := kmsvcredis.AddPending(ctx, s.Redis, queueName, shardID, rec.Partition, rec.Offset); err != nil {
		return Message{}, false, fmt.Errorf("receive message %s: %w", queueName, err)
	}

	receiptHandle := newReceiptHandle(shardID, rec.Partition, rec.Offset)
	groupID := ""
	if meta.FIFO {
		groupID = string(rec.Key)
	}

	if err := kmsvcredis.PutInFlight(ctx, s.Redis, queueName, receiptHandle, kmsvcredis.InFlightRecord{
		ShardID:      shardID,
		Topic:        rec.Topic,
		Partition:    rec.Partition,
		Offset:       rec.Offset,
		GroupID:      groupID,
		Body:         string(rec.Value),
		ReceiveCount: 1,
	}, visTimeout); err != nil {
		return Message{}, false, fmt.Errorf("receive message %s: %w", queueName, err)
	}

	if groupID != "" {
		acquired, err := acquireFIFOSlot(ctx, s.Redis, queueName, groupID, receiptHandle, visTimeout)
		if err != nil {
			return Message{}, false, err
		}
		if !acquired {
			// Another message for this group is already checked out: this
			// one was already consumed off the partition (so it must be
			// tracked, not dropped) but can't be delivered yet — park it on
			// the redeliver list instead of handing it out now.
			if err := kmsvcredis.PushRedeliverable(ctx, s.Redis, queueName, receiptHandle); err != nil {
				return Message{}, false, fmt.Errorf("receive message %s: %w", queueName, err)
			}
			return Message{}, false, nil
		}
	}

	return Message{ReceiptHandle: receiptHandle, Body: string(rec.Value), ReceiveCount: 1}, true, nil
}

func newReceiptHandle(shardID string, partition int32, offset int64) string {
	return fmt.Sprintf("%s:%d:%d:%s", shardID, partition, offset, uuid.NewString())
}
