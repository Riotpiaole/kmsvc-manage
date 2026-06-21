package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer writes to manually-partitioned shard topics: the caller has
// already computed the destination partition via PartitionWithinShard, so
// the client must be configured with a manual partitioner that respects
// Record.Partition rather than re-hashing the key itself.
type Producer struct {
	client *kgo.Client
}

func NewProducer(brokers []string) (*Producer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		return nil, fmt.Errorf("creating kafka producer client: %w", err)
	}
	return &Producer{client: cl}, nil
}

func (p *Producer) Close() {
	p.client.Close()
}

// Produce synchronously writes one record to topic/partition and returns
// the offset it landed at.
func (p *Producer) Produce(ctx context.Context, topic string, partition int32, key, value []byte) (int64, error) {
	rec := &kgo.Record{Topic: topic, Partition: partition, Key: key, Value: value}
	if err := p.client.ProduceSync(ctx, rec).FirstErr(); err != nil {
		return 0, fmt.Errorf("producing to %s/%d: %w", topic, partition, err)
	}
	return rec.Offset, nil
}
