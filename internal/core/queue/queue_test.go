package queue

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/rockliang/kafka-management-service/internal/kafka"
	kmsvcredis "github.com/rockliang/kafka-management-service/internal/redis"
)

// newTestKafka starts an in-memory, wire-protocol-compatible fake Kafka
// cluster (kfake) so these tests exercise the real franz-go producer/
// consumer/admin code paths without needing Docker/testcontainers — not
// available in this environment, same documented tradeoff as the
// queue-operator's envtest substitution (internal/operator).
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

// seedQueue creates the queue's shard-0 topic, queue meta, and shard map
// directly — standing in for the queue-operator (internal/operator), which
// is exercised separately in its own test suite.
func seedQueue(t *testing.T, ctx context.Context, brokers []string, rdb *goredis.Client, name string, fifo bool, partitionsPerShard int32) kafka.Shard {
	t.Helper()
	admin, err := kafka.NewAdmin(brokers)
	if err != nil {
		t.Fatalf("new admin: %v", err)
	}
	t.Cleanup(admin.Close)

	shard := kafka.Shard{ID: "0", Topic: kafka.ShardTopicName(name, fifo, "0"), HashRangeStart: 0, HashRangeEnd: kafka.FullHashRangeEnd, Phase: "Active"}
	if err := admin.CreateTopic(ctx, shard.Topic, kafka.TopicConfig{PartitionCount: partitionsPerShard, ReplicationFactor: 1, RetentionSeconds: 345600, MinInsyncReplicas: 1}); err != nil {
		t.Fatalf("create shard topic: %v", err)
	}
	if err := kmsvcredis.PutQueueMeta(ctx, rdb, name, kmsvcredis.QueueMeta{
		FIFO:                     fifo,
		VisibilityTimeoutSeconds: 1,
		MaxReceiveCount:          3,
		PartitionsPerShard:       partitionsPerShard,
		RetentionSeconds:         345600,
	}); err != nil {
		t.Fatalf("put queue meta: %v", err)
	}
	if err := kmsvcredis.PutShardMap(ctx, rdb, name, []kafka.Shard{shard}); err != nil {
		t.Fatalf("put shard map: %v", err)
	}
	return shard
}

func newTestServices(t *testing.T, brokers []string, rdb *goredis.Client, group string, topics ...string) (*SendMessageService, *ReceiveMessageService, *DeleteMessageService) {
	t.Helper()
	producer, err := kafka.NewProducer(brokers)
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	t.Cleanup(producer.Close)

	consumer, err := kafka.NewConsumer(brokers, group, topics...)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	t.Cleanup(consumer.Close)

	router := &ShardRouter{Redis: rdb}
	send := &SendMessageService{Redis: rdb, Producer: producer, Router: router}
	receive := &ReceiveMessageService{Redis: rdb, Fetcher: consumer, Router: router, PollInterval: 20 * time.Millisecond}
	del := &DeleteMessageService{Redis: rdb}
	return send, receive, del
}

func receiveUntil(t *testing.T, ctx context.Context, receive *ReceiveMessageService, queueName string, want int, waitTime time.Duration) []Message {
	t.Helper()
	out, err := receive.ReceiveMessage(ctx, ReceiveMessageInput{QueueName: queueName, MaxNumberOfMessages: int32(want), WaitTime: waitTime})
	if err != nil {
		t.Fatalf("receive message: %v", err)
	}
	return out
}

func TestSendReceiveDeleteStandardQueue(t *testing.T) {
	ctx := context.Background()
	brokers := newTestKafka(t)
	rdb := newTestRedisClient(t)
	shard := seedQueue(t, ctx, brokers, rdb, "orders", false, 1)
	send, receive, del := newTestServices(t, brokers, rdb, kafka.ConsumerGroup("orders"), shard.Topic)

	if _, err := send.SendMessage(ctx, SendMessageInput{QueueName: "orders", Body: "hello"}); err != nil {
		t.Fatalf("send message: %v", err)
	}

	msgs := receiveUntil(t, ctx, receive, "orders", 1, 2*time.Second)
	if len(msgs) != 1 || msgs[0].Body != "hello" {
		t.Fatalf("messages = %+v, want one %q", msgs, "hello")
	}

	if err := del.DeleteMessage(ctx, "orders", msgs[0].ReceiptHandle); err != nil {
		t.Fatalf("delete message: %v", err)
	}

	// Nothing left to redeliver: a short poll should come back empty.
	again := receiveUntil(t, ctx, receive, "orders", 1, 100*time.Millisecond)
	if len(again) != 0 {
		t.Fatalf("messages after delete = %+v, want none", again)
	}
}

func TestReceiveMessageRedeliversAfterVisibilityTimeoutExpires(t *testing.T) {
	ctx := context.Background()
	brokers := newTestKafka(t)
	rdb := newTestRedisClient(t)
	shard := seedQueue(t, ctx, brokers, rdb, "orders", false, 1)
	send, receive, _ := newTestServices(t, brokers, rdb, kafka.ConsumerGroup("orders"), shard.Topic)

	if _, err := send.SendMessage(ctx, SendMessageInput{QueueName: "orders", Body: "redeliver-me"}); err != nil {
		t.Fatalf("send message: %v", err)
	}

	first := receiveUntil(t, ctx, receive, "orders", 1, 2*time.Second)
	if len(first) != 1 {
		t.Fatalf("first receive = %+v, want one message", first)
	}

	// Run the reaper directly (its own service is covered in internal/redis
	// and a separate Task 7 reaper loop — here we just need its effect: an
	// expired in-flight message becomes redeliverable).
	if _, err := kmsvcredis.Reap(ctx, rdb, "orders", first[0].ReceiptHandle, 3); err != nil {
		t.Fatalf("reap: %v", err)
	}

	second := receiveUntil(t, ctx, receive, "orders", 1, 2*time.Second)
	if len(second) != 1 || second[0].ReceiveCount != 2 {
		t.Fatalf("second receive = %+v, want one message with receiveCount=2", second)
	}
	if second[0].Body != "redeliver-me" {
		t.Fatalf("redelivered body = %q, want %q", second[0].Body, "redeliver-me")
	}
}

func TestFIFOSecondMessageInGroupNotReceivableUntilFirstAcked(t *testing.T) {
	ctx := context.Background()
	brokers := newTestKafka(t)
	rdb := newTestRedisClient(t)
	shard := seedQueue(t, ctx, brokers, rdb, "orders.fifo", true, 1)
	send, receive, del := newTestServices(t, brokers, rdb, kafka.ConsumerGroup("orders.fifo"), shard.Topic)

	if _, err := send.SendMessage(ctx, SendMessageInput{QueueName: "orders.fifo", Body: "first", MessageGroupID: "g1"}); err != nil {
		t.Fatalf("send first: %v", err)
	}
	if _, err := send.SendMessage(ctx, SendMessageInput{QueueName: "orders.fifo", Body: "second", MessageGroupID: "g1"}); err != nil {
		t.Fatalf("send second: %v", err)
	}

	first := receiveUntil(t, ctx, receive, "orders.fifo", 2, 2*time.Second)
	if len(first) != 1 || first[0].Body != "first" {
		t.Fatalf("first receive = %+v, want only %q", first, "first")
	}

	// The second message is consumed off the partition but parked (FIFO gate
	// held) — it must not be receivable yet.
	blocked := receiveUntil(t, ctx, receive, "orders.fifo", 1, 200*time.Millisecond)
	if len(blocked) != 0 {
		t.Fatalf("messages while group locked = %+v, want none", blocked)
	}

	if err := del.DeleteMessage(ctx, "orders.fifo", first[0].ReceiptHandle); err != nil {
		t.Fatalf("delete first: %v", err)
	}

	second := receiveUntil(t, ctx, receive, "orders.fifo", 1, 2*time.Second)
	if len(second) != 1 || second[0].Body != "second" {
		t.Fatalf("second receive after ack = %+v, want %q", second, "second")
	}
}

func TestSendMessageDedupRejectsDuplicateWithinWindow(t *testing.T) {
	ctx := context.Background()
	brokers := newTestKafka(t)
	rdb := newTestRedisClient(t)
	shard := seedQueue(t, ctx, brokers, rdb, "orders.fifo", true, 1)
	send, _, _ := newTestServices(t, brokers, rdb, kafka.ConsumerGroup("orders.fifo"), shard.Topic)

	first, err := send.SendMessage(ctx, SendMessageInput{QueueName: "orders.fifo", Body: "v1", MessageGroupID: "g1", MessageDeduplicationID: "d1"})
	if err != nil {
		t.Fatalf("send first: %v", err)
	}
	dup, err := send.SendMessage(ctx, SendMessageInput{QueueName: "orders.fifo", Body: "v2 (should be ignored)", MessageGroupID: "g1", MessageDeduplicationID: "d1"})
	if err != nil {
		t.Fatalf("send dup: %v", err)
	}
	if dup.MessageID != "d1" {
		t.Errorf("dup message id = %q, want the dedup id %q", dup.MessageID, "d1")
	}
	if first.SequenceNumber == dup.SequenceNumber && first.MessageID == dup.MessageID {
		t.Errorf("expected dup response to be distinguishable from the original send")
	}
}

func TestSendMessageRejectsBodyOverSizeCap(t *testing.T) {
	ctx := context.Background()
	brokers := newTestKafka(t)
	rdb := newTestRedisClient(t)
	shard := seedQueue(t, ctx, brokers, rdb, "orders", false, 1)
	send, _, _ := newTestServices(t, brokers, rdb, kafka.ConsumerGroup("orders"), shard.Topic)

	oversized := make([]byte, MaxMessageBodyBytes+1)
	_, err := send.SendMessage(ctx, SendMessageInput{QueueName: "orders", Body: string(oversized)})
	if err == nil {
		t.Fatal("expected oversized message body to be rejected")
	}
}

func TestFIFOGroupStableAcrossShardSplit(t *testing.T) {
	ctx := context.Background()
	brokers := newTestKafka(t)
	rdb := newTestRedisClient(t)

	admin, err := kafka.NewAdmin(brokers)
	if err != nil {
		t.Fatalf("new admin: %v", err)
	}
	t.Cleanup(admin.Close)

	queueName := "orders.fifo"
	parent := kafka.Shard{ID: "0", Topic: kafka.ShardTopicName(queueName, true, "0"), HashRangeStart: 0, HashRangeEnd: kafka.FullHashRangeEnd, Phase: "Active"}
	for _, topic := range []string{parent.Topic} {
		if err := admin.CreateTopic(ctx, topic, kafka.TopicConfig{PartitionCount: 1, ReplicationFactor: 1, RetentionSeconds: 345600, MinInsyncReplicas: 1}); err != nil {
			t.Fatalf("create topic %s: %v", topic, err)
		}
	}
	if err := kmsvcredis.PutQueueMeta(ctx, rdb, queueName, kmsvcredis.QueueMeta{FIFO: true, VisibilityTimeoutSeconds: 30, MaxReceiveCount: 3, PartitionsPerShard: 1}); err != nil {
		t.Fatalf("put queue meta: %v", err)
	}
	if err := kmsvcredis.PutShardMap(ctx, rdb, queueName, []kafka.Shard{parent}); err != nil {
		t.Fatalf("put shard map: %v", err)
	}

	router := &ShardRouter{Redis: rdb}
	groupID := "group-A"
	beforeShard, err := router.RouteForSend(ctx, queueName, groupID)
	if err != nil {
		t.Fatalf("route before split: %v", err)
	}
	if beforeShard.ID != "0" {
		t.Fatalf("beforeShard = %+v, want shard-0", beforeShard)
	}

	// Simulate the operator splitting shard-0 (internal/operator/shard_split.go).
	mid := kafka.SplitHashRange(parent.HashRangeStart, parent.HashRangeEnd)
	childA := kafka.Shard{ID: "1", Topic: kafka.ShardTopicName(queueName, true, "1"), HashRangeStart: 0, HashRangeEnd: mid, Phase: "Active"}
	childB := kafka.Shard{ID: "2", Topic: kafka.ShardTopicName(queueName, true, "2"), HashRangeStart: mid, HashRangeEnd: kafka.FullHashRangeEnd, Phase: "Active"}
	parent.Phase = "Closing"
	for _, topic := range []string{childA.Topic, childB.Topic} {
		if err := admin.CreateTopic(ctx, topic, kafka.TopicConfig{PartitionCount: 1, ReplicationFactor: 1, RetentionSeconds: 345600, MinInsyncReplicas: 1}); err != nil {
			t.Fatalf("create topic %s: %v", topic, err)
		}
	}
	if err := kmsvcredis.PutShardMap(ctx, rdb, queueName, []kafka.Shard{parent, childA, childB}); err != nil {
		t.Fatalf("put post-split shard map: %v", err)
	}

	afterShard, err := router.RouteForSend(ctx, queueName, groupID)
	if err != nil {
		t.Fatalf("route after split: %v", err)
	}
	h := kafka.HashKey(groupID)
	wantID := "1"
	if h >= mid {
		wantID = "2"
	}
	if afterShard.ID != wantID {
		t.Fatalf("afterShard = %+v, want shard %s (hash=%d, mid=%d)", afterShard, wantID, h, mid)
	}

	// ReceiveMessage consuming across both the still-draining parent and the
	// new child must still see every message, regardless of which shard it
	// landed on.
	send, receive, _ := newTestServices(t, brokers, rdb, kafka.ConsumerGroup(queueName), parent.Topic, childA.Topic, childB.Topic)
	if _, err := send.SendMessage(ctx, SendMessageInput{QueueName: queueName, Body: "post-split", MessageGroupID: groupID}); err != nil {
		t.Fatalf("send post-split: %v", err)
	}
	msgs := receiveUntil(t, ctx, receive, queueName, 1, 2*time.Second)
	if len(msgs) != 1 || msgs[0].Body != "post-split" {
		t.Fatalf("messages = %+v, want one %q", msgs, "post-split")
	}
}
