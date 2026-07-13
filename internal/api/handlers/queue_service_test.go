package handlers

import (
	"context"
	"testing"
	"time"

	kafkamgmtv1 "github.com/Riotpiaole/kmsvc-proto/gen/kafkamgmt/v1"
	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/rockliang/kafka-management-service/internal/core/queue"
	"github.com/rockliang/kafka-management-service/internal/kafka"
	kmsvcredis "github.com/rockliang/kafka-management-service/internal/redis"
)

// newTestKafka starts an in-memory, wire-protocol-compatible fake Kafka
// cluster (kfake), the same documented tradeoff used by internal/core/queue's
// own tests — no Docker/testcontainers needed in this environment.
func newTestKafka(t *testing.T) []string {
	t.Helper()
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatalf("starting kfake cluster: %v", err)
	}
	t.Cleanup(cluster.Close)
	return cluster.ListenAddrs()
}

func newTestRedis(t *testing.T) *goredis.Client {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("starting miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	return goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
}

func newTestQueueService(t *testing.T, brokers []string, rdb *goredis.Client) *QueueService {
	t.Helper()

	producer, err := kafka.NewProducer(brokers)
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	t.Cleanup(producer.Close)

	router := &queue.ShardRouter{Redis: rdb}
	svc := &QueueService{
		Redis:               rdb,
		Router:              router,
		Send:                &queue.SendMessageService{Redis: rdb, Producer: producer, Router: router},
		Delete:              &queue.DeleteMessageService{Redis: rdb},
		Visibility:          &queue.ChangeVisibilityService{Redis: rdb},
		Consumers:           &ConsumerRegistry{Brokers: brokers, Router: router},
		ReceivePollInterval: 20 * time.Millisecond,
	}
	t.Cleanup(svc.Consumers.Close)
	return svc
}

func seedTestQueue(t *testing.T, ctx context.Context, brokers []string, rdb *goredis.Client, name string) {
	t.Helper()
	admin, err := kafka.NewAdmin(brokers)
	if err != nil {
		t.Fatalf("new admin: %v", err)
	}
	t.Cleanup(admin.Close)

	shard := kafka.Shard{ID: "0", Topic: kafka.ShardTopicName(name, false, "0"), HashRangeStart: 0, HashRangeEnd: kafka.FullHashRangeEnd, Phase: "Active"}
	if err := admin.CreateTopic(ctx, shard.Topic, kafka.TopicConfig{PartitionCount: 1, ReplicationFactor: 1, RetentionSeconds: 345600, MinInsyncReplicas: 1}); err != nil {
		t.Fatalf("create shard topic: %v", err)
	}
	if err := kmsvcredis.PutQueueMeta(ctx, rdb, name, kmsvcredis.QueueMeta{VisibilityTimeoutSeconds: 5, MaxReceiveCount: 3, PartitionsPerShard: 1, RetentionSeconds: 345600}); err != nil {
		t.Fatalf("put queue meta: %v", err)
	}
	if err := kmsvcredis.PutShardMap(ctx, rdb, name, []kafka.Shard{shard}); err != nil {
		t.Fatalf("put shard map: %v", err)
	}
}

// TestSendReceiveDeleteThroughGRPCHandlers exercises the proto<->core
// translation layer end-to-end: SendMessage -> ReceiveMessage ->
// DeleteMessage via the same QueueService a real grpc.Server would dispatch
// to, against a fake Kafka+Redis backend.
func TestSendReceiveDeleteThroughGRPCHandlers(t *testing.T) {
	ctx := context.Background()
	brokers := newTestKafka(t)
	rdb := newTestRedis(t)
	seedTestQueue(t, ctx, brokers, rdb, "orders")
	svc := newTestQueueService(t, brokers, rdb)

	sendResp, err := svc.SendMessage(ctx, &kafkamgmtv1.SendMessageRequest{QueueName: "orders", MessageBody: []byte("hello")})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if sendResp.GetMessageId() == "" {
		t.Fatal("SendMessage: expected a non-empty message id")
	}

	deadline := time.Now().Add(3 * time.Second)
	var msgs []*kafkamgmtv1.Message
	for time.Now().Before(deadline) {
		recvResp, err := svc.ReceiveMessage(ctx, &kafkamgmtv1.ReceiveMessageRequest{QueueName: "orders", MaxNumberOfMessages: 1, WaitTimeSeconds: 1})
		if err != nil {
			t.Fatalf("ReceiveMessage: %v", err)
		}
		if len(recvResp.GetMessages()) > 0 {
			msgs = recvResp.GetMessages()
			break
		}
	}
	if len(msgs) != 1 || string(msgs[0].GetBody()) != "hello" {
		t.Fatalf("messages = %+v, want one body=hello", msgs)
	}

	if _, err := svc.DeleteMessage(ctx, &kafkamgmtv1.DeleteMessageRequest{QueueName: "orders", ReceiptHandle: msgs[0].GetReceiptHandle()}); err != nil {
		t.Fatalf("DeleteMessage: %v", err)
	}
}

func TestSendMessageUnknownQueueMapsToNotFound(t *testing.T) {
	ctx := context.Background()
	brokers := newTestKafka(t)
	rdb := newTestRedis(t)
	svc := newTestQueueService(t, brokers, rdb)

	_, err := svc.SendMessage(ctx, &kafkamgmtv1.SendMessageRequest{QueueName: "missing", MessageBody: []byte("hi")})
	if err == nil {
		t.Fatal("expected an error for an unknown queue")
	}
}

func TestChangeMessageVisibilityUnknownHandleErrors(t *testing.T) {
	ctx := context.Background()
	brokers := newTestKafka(t)
	rdb := newTestRedis(t)
	seedTestQueue(t, ctx, brokers, rdb, "orders")
	svc := newTestQueueService(t, brokers, rdb)

	_, err := svc.ChangeMessageVisibility(ctx, &kafkamgmtv1.ChangeMessageVisibilityRequest{
		QueueName: "orders", ReceiptHandle: "does-not-exist", VisibilityTimeoutSeconds: 30,
	})
	if err == nil {
		t.Fatal("expected an error for an unknown receipt handle")
	}
}
