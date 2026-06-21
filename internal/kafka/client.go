package kafka

import (
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// NewProducerClient builds a producer-only client. Callers own Close().
func NewProducerClient(brokers []string) (*kgo.Client, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
	)
	if err != nil {
		return nil, fmt.Errorf("creating kafka producer client: %w", err)
	}
	return cl, nil
}

// NewConsumerClient builds a client that consumes the given topics as part
// of consumerGroup. The message-plane service relies on Kafka's native
// consumer-group rebalancing for partition ownership across replicas
// (design.md §9) — offset commit is driven manually via the low-watermark
// strategy in design.md §3, so auto-commit is disabled.
func NewConsumerClient(brokers []string, consumerGroup string, topics ...string) (*kgo.Client, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(consumerGroup),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, fmt.Errorf("creating kafka consumer client: %w", err)
	}
	return cl, nil
}
