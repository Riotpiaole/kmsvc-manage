package operator

import (
	"context"

	"github.com/rockliang/kafka-management-service/internal/kafka"
)

// TopicAdmin is the subset of internal/kafka.Admin the reconciler needs,
// abstracted so tests can substitute a fake instead of a real Kafka cluster.
type TopicAdmin interface {
	CreateTopic(ctx context.Context, topic string, cfg kafka.TopicConfig) error
	DeleteTopic(ctx context.Context, topic string) error
	LogEndOffsetSum(ctx context.Context, topic string) (int64, error)
	ConsumerLag(ctx context.Context, group, topic string) (int64, error)
	ReplicaBrokerIDs(ctx context.Context, topic string) ([]int32, error)
}
