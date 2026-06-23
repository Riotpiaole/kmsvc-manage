package kafka

import (
	"context"
	"fmt"
	"strconv"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Admin wraps kadm.Client with the topic conventions from design.md §6.
type Admin struct {
	client *kadm.Client
}

// NewAdmin builds an Admin from a set of broker addresses.
func NewAdmin(brokers []string) (*Admin, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, fmt.Errorf("creating kafka client: %w", err)
	}
	return &Admin{client: kadm.NewClient(cl)}, nil
}

// TopicConfig is the subset of Kafka topic configuration design.md §6 cares about.
type TopicConfig struct {
	PartitionCount    int32
	ReplicationFactor int16
	RetentionSeconds  int32
	MinInsyncReplicas int16
}

// CreateTopic creates a topic idempotently: if it already exists with the
// requested partition count, this is a no-op (design.md §3 task 3
// acceptance criteria).
func (a *Admin) CreateTopic(ctx context.Context, topic string, cfg TopicConfig) error {
	configs := map[string]*string{
		"retention.ms":        strPtr(strconv.FormatInt(int64(cfg.RetentionSeconds)*1000, 10)),
		"cleanup.policy":      strPtr("delete"),
		"min.insync.replicas": strPtr(strconv.Itoa(int(cfg.MinInsyncReplicas))),
	}

	resp, err := a.client.CreateTopics(ctx, cfg.PartitionCount, cfg.ReplicationFactor, configs, topic)
	if err != nil {
		return fmt.Errorf("create topic %q: %w", topic, err)
	}
	for _, t := range resp {
		if t.Err != nil && !isTopicExistsErr(t.Err) {
			return fmt.Errorf("create topic %q: %w", topic, t.Err)
		}
	}
	return nil
}

// DeleteTopic deletes a topic. Deleting a non-existent topic is not an error.
func (a *Admin) DeleteTopic(ctx context.Context, topic string) error {
	resp, err := a.client.DeleteTopics(ctx, topic)
	if err != nil {
		return fmt.Errorf("delete topic %q: %w", topic, err)
	}
	for _, t := range resp {
		if t.Err != nil && !isUnknownTopicErr(t.Err) {
			return fmt.Errorf("delete topic %q: %w", topic, t.Err)
		}
	}
	return nil
}

// DescribeTopic returns the live partition count and config for a topic, or
// ok=false if it does not exist.
func (a *Admin) DescribeTopic(ctx context.Context, topic string) (partitions int32, ok bool, err error) {
	td, err := a.client.ListTopics(ctx, topic)
	if err != nil {
		return 0, false, fmt.Errorf("describe topic %q: %w", topic, err)
	}
	detail, found := td[topic]
	if !found || detail.Err != nil {
		return 0, false, nil
	}
	return int32(len(detail.Partitions)), true, nil
}

// ReplicaBrokerIDs returns the set of broker IDs holding any replica of any
// partition of topic, used by the queue-operator to resolve which
// availability zones (node labels) a shard's data actually lives on
// (design.md §2a AZ-awareness).
func (a *Admin) ReplicaBrokerIDs(ctx context.Context, topic string) ([]int32, error) {
	td, err := a.client.ListTopics(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("list topics %q: %w", topic, err)
	}
	detail, found := td[topic]
	if !found || detail.Err != nil {
		return nil, nil
	}

	seen := make(map[int32]bool)
	var ids []int32
	for _, p := range detail.Partitions {
		for _, id := range p.Replicas {
			if !seen[id] {
				seen[id] = true
				ids = append(ids, id)
			}
		}
	}
	return ids, nil
}

// LogEndOffsetSum returns the sum of the high-watermark offsets across every
// partition of a topic — a monotonically increasing proxy for total records
// produced, used by the queue-operator's shard-split sampler (design.md §2c)
// to estimate throughput between two reconcile ticks.
func (a *Admin) LogEndOffsetSum(ctx context.Context, topic string) (int64, error) {
	offsets, err := a.client.ListEndOffsets(ctx, topic)
	if err != nil {
		return 0, fmt.Errorf("list end offsets %q: %w", topic, err)
	}
	var sum int64
	for _, partitions := range offsets {
		for _, o := range partitions {
			if o.Err != nil {
				continue
			}
			sum += o.Offset
		}
	}
	return sum, nil
}

// ConsumerLag returns the total lag (sum across partitions) of a consumer
// group against a topic, used by the queue-operator to decide when a
// `Closing` shard (design.md §2c) is fully drained.
func (a *Admin) ConsumerLag(ctx context.Context, group, topic string) (int64, error) {
	lags, err := a.client.Lag(ctx, group)
	if err != nil {
		return 0, fmt.Errorf("lag for group %q: %w", group, err)
	}
	described, ok := lags[group]
	if !ok || described.Error() != nil {
		return 0, nil
	}
	return described.Lag.TotalByTopic()[topic].Lag, nil
}

// CommitOffset commits the next offset to read for (group, topic, partition)
// to Kafka's real consumer-group offsets — the message plane calls this on
// ack to advance the low-watermark commit described in design.md §3, rather
// than relying on the consumer client's own interval autocommit (which is
// disabled, see internal/kafka.Consumer).
func (a *Admin) CommitOffset(ctx context.Context, group, topic string, partition int32, offset int64) error {
	offsets := kadm.Offsets{topic: {partition: kadm.Offset{Topic: topic, Partition: partition, At: offset}}}
	resp, err := a.client.CommitOffsets(ctx, group, offsets)
	if err != nil {
		return fmt.Errorf("commit offset %s/%s/%d: %w", group, topic, partition, err)
	}
	if err := resp.Error(); err != nil {
		return fmt.Errorf("commit offset %s/%s/%d: %w", group, topic, partition, err)
	}
	return nil
}

func (a *Admin) Close() {
	a.client.Close()
}

func strPtr(s string) *string { return &s }

func isTopicExistsErr(err error) bool {
	return err != nil && (err.Error() == "TOPIC_ALREADY_EXISTS" || containsCode(err, "TOPIC_ALREADY_EXISTS"))
}

func isUnknownTopicErr(err error) bool {
	return err != nil && containsCode(err, "UNKNOWN_TOPIC_OR_PARTITION")
}

func containsCode(err error, code string) bool {
	return err != nil && (err.Error() == code || len(err.Error()) >= len(code) && (err.Error()[:len(code)] == code))
}
