package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"

	"github.com/rockliang/kafka-management-service/internal/kafka"
	kmsvcredis "github.com/rockliang/kafka-management-service/internal/redis"
)

// MaxMessageBodyBytes is the SQS-compatible size cap enforced at
// SendMessage, design.md §6.
const MaxMessageBodyBytes = 256 * 1024

const defaultDedupWindow = 5 * time.Minute

// Producer is the subset of kafka.Producer SendMessage needs, abstracted so
// tests can substitute a fake without a real Kafka broker.
type Producer interface {
	Produce(ctx context.Context, topic string, partition int32, key, value []byte) (offset int64, err error)
}

type SendMessageInput struct {
	QueueName              string
	Body                   string
	MessageGroupID         string // FIFO only
	MessageDeduplicationID string // FIFO only
}

type SendMessageOutput struct {
	MessageID      string
	SequenceNumber int64 // the Kafka offset it landed at
}

type SendMessageService struct {
	Redis       *goredis.Client
	Producer    Producer
	Router      *ShardRouter
	DedupWindow time.Duration
}

// SendMessage implements SendMessage (design.md §2b/§2c/§6): enforces the
// size cap, runs the FIFO dedup check, resolves the destination shard via
// the routing key, and produces to that shard's topic at the
// within-shard partition the key hashes to.
func (s *SendMessageService) SendMessage(ctx context.Context, in SendMessageInput) (SendMessageOutput, error) {
	if len(in.Body) > MaxMessageBodyBytes {
		return SendMessageOutput{}, fmt.Errorf("message body exceeds %d bytes", MaxMessageBodyBytes)
	}

	meta, ok, err := kmsvcredis.GetQueueMeta(ctx, s.Redis, in.QueueName)
	if err != nil {
		return SendMessageOutput{}, fmt.Errorf("send message %s: %w", in.QueueName, err)
	}
	if !ok {
		return SendMessageOutput{}, fmt.Errorf("queue %s not found", in.QueueName)
	}

	if meta.FIFO {
		if in.MessageGroupID == "" {
			return SendMessageOutput{}, fmt.Errorf("messageGroupId is required for FIFO queue %s", in.QueueName)
		}
		if in.MessageDeduplicationID != "" {
			window := s.DedupWindow
			if window <= 0 {
				window = defaultDedupWindow
			}
			fresh, err := kmsvcredis.TryDedup(ctx, s.Redis, in.QueueName, in.MessageGroupID, in.MessageDeduplicationID, window)
			if err != nil {
				return SendMessageOutput{}, fmt.Errorf("send message %s: %w", in.QueueName, err)
			}
			if !fresh {
				// SQS returns the original send's message ID for a deduped
				// retry; v1 doesn't track that mapping, so the dedup ID
				// itself is returned as a stable, idempotent identifier.
				return SendMessageOutput{MessageID: in.MessageDeduplicationID}, nil
			}
		}
	}

	routingKey := in.MessageGroupID
	if routingKey == "" {
		routingKey = uuid.NewString()
	}

	shard, err := s.Router.RouteForSend(ctx, in.QueueName, routingKey)
	if err != nil {
		return SendMessageOutput{}, err
	}

	partition := kafka.PartitionWithinShard(routingKey, meta.PartitionsPerShard)
	var key []byte
	if meta.FIFO {
		key = []byte(in.MessageGroupID)
	}

	offset, err := s.Producer.Produce(ctx, shard.Topic, partition, key, []byte(in.Body))
	if err != nil {
		return SendMessageOutput{}, fmt.Errorf("send message %s: %w", in.QueueName, err)
	}

	return SendMessageOutput{
		MessageID:      uuid.NewString(),
		SequenceNumber: offset,
	}, nil
}
