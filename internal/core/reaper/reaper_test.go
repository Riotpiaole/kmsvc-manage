package reaper

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/rockliang/kafka-management-service/internal/core/queue"
	"github.com/rockliang/kafka-management-service/internal/kafka"
	kmsvcredis "github.com/rockliang/kafka-management-service/internal/redis"
)

// newTestKafka mirrors internal/core/queue's test setup: an in-memory,
// wire-protocol-compatible fake Kafka cluster (kfake) instead of
// testcontainers/Docker, exercising the real franz-go producer/consumer/
// admin code paths the DLQ-routing path depends on.
func newTestKafka(t *testing.T) []string {
	t.Helper()
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatalf("starting kfake cluster: %v", err)
	}
	t.Cleanup(cluster.Close)
	return cluster.ListenAddrs()
}

func newTestRedisClient(t *testing.T) *goredis.Client {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("starting miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	return goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
}

// seedQueue creates a queue's shard-0 topic, queue meta, and shard map
// directly — standing in for the queue-operator, exercised in its own test
// suite (internal/operator).
func seedQueue(t *testing.T, ctx context.Context, admin *kafka.Admin, rdb *goredis.Client, name string, maxReceiveCount int32, dlqName string) kafka.Shard {
	t.Helper()
	shard := kafka.Shard{ID: "0", Topic: kafka.ShardTopicName(name, false, "0"), HashRangeStart: 0, HashRangeEnd: kafka.FullHashRangeEnd, Phase: "Active"}
	if err := admin.CreateTopic(ctx, shard.Topic, kafka.TopicConfig{PartitionCount: 1, ReplicationFactor: 1, RetentionSeconds: 345600, MinInsyncReplicas: 1}); err != nil {
		t.Fatalf("create shard topic: %v", err)
	}
	if err := kmsvcredis.PutQueueMeta(ctx, rdb, name, kmsvcredis.QueueMeta{
		VisibilityTimeoutSeconds: 30,
		MaxReceiveCount:          maxReceiveCount,
		DLQQueueName:             dlqName,
		PartitionsPerShard:       1,
		RetentionSeconds:         345600,
	}); err != nil {
		t.Fatalf("put queue meta: %v", err)
	}
	if err := kmsvcredis.PutShardMap(ctx, rdb, name, []kafka.Shard{shard}); err != nil {
		t.Fatalf("put shard map: %v", err)
	}
	return shard
}

// putExpiredInFlight records an in-flight message whose visibility deadline
// is already in the past, so it's immediately eligible for the sweep —
// standing in for ReceiveMessage handing out a message and the caller never
// acking it. PutInFlight is called with a normal positive visibility timeout
// (it sets the in-flight hash's TTL to timeout+1min; a negative timeout
// would give a negative TTL, which deletes the hash immediately rather than
// leaving it in place for the reaper to find) and the vis_index score is
// then backdated directly to simulate the deadline having already elapsed.
func putExpiredInFlight(t *testing.T, ctx context.Context, rdb *goredis.Client, queueName, receiptHandle string, rec kmsvcredis.InFlightRecord) {
	t.Helper()
	if err := kmsvcredis.PutInFlight(ctx, rdb, queueName, receiptHandle, rec, time.Minute); err != nil {
		t.Fatalf("put expired in-flight: %v", err)
	}
	expired := time.Now().Add(-time.Hour).UnixMilli()
	if err := rdb.ZAdd(ctx, kmsvcredis.VisIndexKey(queueName), goredis.Z{Score: float64(expired), Member: receiptHandle}).Err(); err != nil {
		t.Fatalf("backdate vis_index: %v", err)
	}
}

func TestSweepRoutesMaxedOutMessageToDLQ(t *testing.T) {
	ctx := context.Background()
	brokers := newTestKafka(t)
	rdb := newTestRedisClient(t)

	admin, err := kafka.NewAdmin(brokers)
	if err != nil {
		t.Fatalf("new admin: %v", err)
	}
	t.Cleanup(admin.Close)

	shard := seedQueue(t, ctx, admin, rdb, "orders", 1, "orders-dlq")
	dlqShard := seedQueue(t, ctx, admin, rdb, "orders-dlq", 3, "")

	producer, err := kafka.NewProducer(brokers)
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	t.Cleanup(producer.Close)
	router := &queue.ShardRouter{Redis: rdb}
	sender := &queue.SendMessageService{Redis: rdb, Producer: producer, Router: router}

	putExpiredInFlight(t, ctx, rdb, "orders", "h1", kmsvcredis.InFlightRecord{
		ShardID: shard.ID, Topic: shard.Topic, Partition: 0, Offset: 0,
		ReceiveCount: 1, Body: "maxed-out",
	})

	r := &Reaper{Redis: rdb, DLQSender: sender}
	swept, err := r.Sweep(ctx, "orders")
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if swept != 1 {
		t.Fatalf("swept = %d, want 1", swept)
	}

	// The in-flight record and vis_index entry must be fully cleaned up.
	if _, ok, err := kmsvcredis.GetInFlight(ctx, rdb, "orders", "h1"); err != nil || ok {
		t.Fatalf("in-flight record still present after DLQ-route: ok=%v err=%v", ok, err)
	}

	consumer, err := kafka.NewConsumer(brokers, "verify-dlq", dlqShard.Topic)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	t.Cleanup(consumer.Close)
	pollCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var body string
	for body == "" {
		records, err := consumer.Poll(pollCtx)
		if err != nil {
			t.Fatalf("poll dlq topic: %v", err)
		}
		for _, rec := range records {
			body = string(rec.Value)
		}
		if pollCtx.Err() != nil {
			break
		}
	}
	if body != "maxed-out" {
		t.Fatalf("dlq message body = %q, want %q", body, "maxed-out")
	}
}

func TestSweepRedeliversWhenUnderMaxReceiveCount(t *testing.T) {
	ctx := context.Background()
	brokers := newTestKafka(t)
	rdb := newTestRedisClient(t)

	admin, err := kafka.NewAdmin(brokers)
	if err != nil {
		t.Fatalf("new admin: %v", err)
	}
	t.Cleanup(admin.Close)

	shard := seedQueue(t, ctx, admin, rdb, "orders", 3, "")
	putExpiredInFlight(t, ctx, rdb, "orders", "h1", kmsvcredis.InFlightRecord{
		ShardID: shard.ID, Topic: shard.Topic, Partition: 0, Offset: 0,
		ReceiveCount: 1, Body: "retry-me",
	})

	r := &Reaper{Redis: rdb}
	swept, err := r.Sweep(ctx, "orders")
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if swept != 1 {
		t.Fatalf("swept = %d, want 1", swept)
	}

	rec, ok, err := kmsvcredis.GetInFlight(ctx, rdb, "orders", "h1")
	if err != nil || !ok {
		t.Fatalf("in-flight record missing after redeliver: ok=%v err=%v", ok, err)
	}
	if rec.ReceiveCount != 2 {
		t.Fatalf("receiveCount = %d, want 2", rec.ReceiveCount)
	}

	handle, ok, err := kmsvcredis.PopRedeliverable(ctx, rdb, "orders")
	if err != nil || !ok || handle != "h1" {
		t.Fatalf("redeliverable = (%q, %v), want (%q, true)", handle, ok, "h1")
	}
}

// TestConcurrentSweepsProduceNoDuplicateDLQWrites covers acceptance criterion
// 3 of Task 7 at the reaper level (Task 4's Redis-layer concurrency test
// covers reap.lua's atomicity directly): two reaper instances racing the
// same expired entry must yield exactly one DLQ write.
func TestConcurrentSweepsProduceNoDuplicateDLQWrites(t *testing.T) {
	ctx := context.Background()
	brokers := newTestKafka(t)
	rdb := newTestRedisClient(t)

	admin, err := kafka.NewAdmin(brokers)
	if err != nil {
		t.Fatalf("new admin: %v", err)
	}
	t.Cleanup(admin.Close)

	shard := seedQueue(t, ctx, admin, rdb, "orders", 1, "orders-dlq")
	seedQueue(t, ctx, admin, rdb, "orders-dlq", 3, "")

	producer, err := kafka.NewProducer(brokers)
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	t.Cleanup(producer.Close)
	router := &queue.ShardRouter{Redis: rdb}

	var mu sync.Mutex
	sendCount := 0
	countingSender := countingDLQSender{
		inner: &queue.SendMessageService{Redis: rdb, Producer: producer, Router: router},
		onSend: func() {
			mu.Lock()
			sendCount++
			mu.Unlock()
		},
	}

	putExpiredInFlight(t, ctx, rdb, "orders", "h1", kmsvcredis.InFlightRecord{
		ShardID: shard.ID, Topic: shard.Topic, Partition: 0, Offset: 0,
		ReceiveCount: 1, Body: "race-me",
	})

	r1 := &Reaper{Redis: rdb, DLQSender: countingSender}
	r2 := &Reaper{Redis: rdb, DLQSender: countingSender}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _, _ = r1.Sweep(ctx, "orders") }()
	go func() { defer wg.Done(); _, _ = r2.Sweep(ctx, "orders") }()
	wg.Wait()

	if sendCount != 1 {
		t.Fatalf("dlq send count = %d, want exactly 1", sendCount)
	}
}

type countingDLQSender struct {
	inner  DLQSender
	onSend func()
}

func (c countingDLQSender) SendMessage(ctx context.Context, in queue.SendMessageInput) (queue.SendMessageOutput, error) {
	c.onSend()
	return c.inner.SendMessage(ctx, in)
}
