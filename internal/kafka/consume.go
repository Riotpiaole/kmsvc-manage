package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Record is a fetched Kafka record relevant to the message plane.
type Record struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
}

// Consumer wraps a kgo.Client consuming a queue's shard topics under the
// shared consumer group (design.md §3 ConsumerGroup). Autocommit is
// disabled: offset commits are driven by the watermark logic in
// internal/core/queue, not by the client's own interval commit, so a
// crash between receive and ack never advances the committed offset past an
// unacked message (design.md §3's at-least-once guarantee).
type Consumer struct {
	client *kgo.Client
}

// NewConsumer subscribes to the given shard topics as a real consumer-group
// member: Kafka's group-rebalance protocol handles partition ownership
// across replicas (design.md §9), and the queue-operator's drain check
// (internal/kafka.Admin.ConsumerLag) reads this same group's committed
// offsets to know when a `Closing` shard is safe to delete.
func NewConsumer(brokers []string, group string, topics ...string) (*Consumer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, fmt.Errorf("creating kafka consumer client: %w", err)
	}
	return &Consumer{client: cl}, nil
}

// AddTopics subscribes to additional shard topics created by a split,
// without losing the group membership/offsets already held for existing
// topics.
func (c *Consumer) AddTopics(topics ...string) {
	c.client.AddConsumeTopics(topics...)
}

func (c *Consumer) Close() {
	c.client.Close()
}

// Poll runs one non-blocking fetch iteration and returns whatever records
// were immediately available. The long-poll wait loop lives in
// internal/core/queue.ReceiveMessageService, not here.
func (c *Consumer) Poll(ctx context.Context) ([]Record, error) {
	fetches := c.client.PollFetches(ctx)
	if errs := fetches.Errors(); len(errs) > 0 {
		return nil, fmt.Errorf("poll fetches: %w", errs[0].Err)
	}
	var out []Record
	fetches.EachRecord(func(r *kgo.Record) {
		out = append(out, Record{
			Topic:     r.Topic,
			Partition: r.Partition,
			Offset:    r.Offset,
			Key:       r.Key,
			Value:     r.Value,
		})
	})
	return out, nil
}
