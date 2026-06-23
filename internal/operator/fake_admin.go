package operator

import (
	"context"
	"sync"

	"github.com/rockliang/kafka-management-service/internal/kafka"
)

// fakeAdmin is an in-memory TopicAdmin for reconciler tests — avoids needing
// a real Kafka cluster (or envtest's apiserver binaries, which aren't
// available in this environment) just to exercise reconcile logic.
type fakeAdmin struct {
	mu         sync.Mutex
	topics     map[string]kafka.TopicConfig
	offsetSums map[string]int64
	lag        map[string]int64 // keyed by group+"/"+topic
}

func newFakeAdmin() *fakeAdmin {
	return &fakeAdmin{
		topics:     map[string]kafka.TopicConfig{},
		offsetSums: map[string]int64{},
		lag:        map[string]int64{},
	}
}

func (f *fakeAdmin) CreateTopic(_ context.Context, topic string, cfg kafka.TopicConfig) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.topics[topic] = cfg
	return nil
}

func (f *fakeAdmin) DeleteTopic(_ context.Context, topic string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.topics, topic)
	return nil
}

func (f *fakeAdmin) LogEndOffsetSum(_ context.Context, topic string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.offsetSums[topic], nil
}

func (f *fakeAdmin) ConsumerLag(_ context.Context, group, topic string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lag[group+"/"+topic], nil
}

func (f *fakeAdmin) ReplicaBrokerIDs(_ context.Context, topic string) ([]int32, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.topics[topic]; !ok {
		return nil, nil
	}
	return []int32{0}, nil
}

func (f *fakeAdmin) setOffsetSum(topic string, v int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.offsetSums[topic] = v
}

func (f *fakeAdmin) setLag(group, topic string, v int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lag[group+"/"+topic] = v
}

func (f *fakeAdmin) hasTopic(topic string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.topics[topic]
	return ok
}
