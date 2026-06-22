package handlers

import (
	"context"
	"fmt"
	"sync"

	"github.com/rockliang/kafka-management-service/internal/core/queue"
	"github.com/rockliang/kafka-management-service/internal/kafka"
)

// ConsumerRegistry lazily creates and caches one Kafka consumer per queue,
// subscribed to that queue's currently-consumable shard topics (design.md
// §6 — every Active+Closing shard). It is the only place ReceiveMessage's
// gRPC handler needs to know about per-queue Kafka clients.
//
// Known v1 limitation (tracked in design.md §11 item 5): if the operator
// splits a shard after a queue's consumer was created, the new child topics
// are picked up the next time Get is called for that queue (each call
// re-syncs against the live shard map), but there is a short window between
// a split and the next ReceiveMessage call where the consumer hasn't yet
// subscribed to the new topics.
type ConsumerRegistry struct {
	Brokers []string
	Router  *queue.ShardRouter

	mu        sync.Mutex
	consumers map[string]*registeredConsumer
}

type registeredConsumer struct {
	consumer *kafka.Consumer
	topics   map[string]bool
}

// Get returns a Fetcher subscribed to queueName's current consumable shard
// topics, creating the underlying Kafka consumer-group client on first use.
func (r *ConsumerRegistry) Get(ctx context.Context, queueName string) (queue.Fetcher, error) {
	shards, err := r.Router.ConsumableShards(ctx, queueName)
	if err != nil {
		return nil, err
	}
	if len(shards) == 0 {
		return nil, fmt.Errorf("queue %s has no consumable shards", queueName)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.consumers == nil {
		r.consumers = make(map[string]*registeredConsumer)
	}

	rc, ok := r.consumers[queueName]
	if !ok {
		topics := make([]string, 0, len(shards))
		topicSet := make(map[string]bool, len(shards))
		for _, s := range shards {
			topics = append(topics, s.Topic)
			topicSet[s.Topic] = true
		}
		cl, err := kafka.NewConsumer(r.Brokers, kafka.ConsumerGroup(queueName), topics...)
		if err != nil {
			return nil, fmt.Errorf("create consumer for queue %s: %w", queueName, err)
		}
		rc = &registeredConsumer{consumer: cl, topics: topicSet}
		r.consumers[queueName] = rc
		return rc.consumer, nil
	}

	var newTopics []string
	for _, s := range shards {
		if !rc.topics[s.Topic] {
			newTopics = append(newTopics, s.Topic)
			rc.topics[s.Topic] = true
		}
	}
	if len(newTopics) > 0 {
		rc.consumer.AddTopics(newTopics...)
	}
	return rc.consumer, nil
}

// Close closes every consumer this registry has created, used on server
// shutdown.
func (r *ConsumerRegistry) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, rc := range r.consumers {
		rc.consumer.Close()
	}
}
