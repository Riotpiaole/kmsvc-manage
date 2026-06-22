package redis

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"github.com/rockliang/kafka-management-service/internal/kafka"
)

func newTestClient(t *testing.T) *goredis.Client {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("starting miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	return goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
}

func TestDedupFirstAcceptsSecondRejects(t *testing.T) {
	rdb := newTestClient(t)
	ctx := context.Background()

	first, err := TryDedup(ctx, rdb, "orders", "g1", "d1", 5*time.Minute)
	if err != nil || !first {
		t.Fatalf("first dedup attempt: ok=%v err=%v, want ok=true", first, err)
	}
	second, err := TryDedup(ctx, rdb, "orders", "g1", "d1", 5*time.Minute)
	if err != nil || second {
		t.Fatalf("second dedup attempt: ok=%v err=%v, want ok=false", second, err)
	}
}

func TestInFlightPutGetExtend(t *testing.T) {
	rdb := newTestClient(t)
	ctx := context.Background()

	rec := InFlightRecord{ShardID: "0", Topic: "kmsvc.orders.shard-0", Partition: 2, Offset: 42, GroupID: "g1", Body: "hello"}
	if err := PutInFlight(ctx, rdb, "orders", "rh-1", rec, 30*time.Second); err != nil {
		t.Fatalf("PutInFlight: %v", err)
	}

	got, ok, err := GetInFlight(ctx, rdb, "orders", "rh-1")
	if err != nil || !ok {
		t.Fatalf("GetInFlight: ok=%v err=%v", ok, err)
	}
	if got.Topic != rec.Topic || got.Offset != rec.Offset || got.Body != rec.Body {
		t.Errorf("GetInFlight = %+v, want %+v", got, rec)
	}

	extended, err := ExtendVisibility(ctx, rdb, "orders", "rh-1", time.Minute)
	if err != nil || !extended {
		t.Fatalf("ExtendVisibility: ok=%v err=%v", extended, err)
	}

	missing, err := ExtendVisibility(ctx, rdb, "orders", "rh-missing", time.Minute)
	if err != nil || missing {
		t.Fatalf("ExtendVisibility(missing): ok=%v err=%v, want ok=false", missing, err)
	}
}

func TestWatermarkAdvancesAsPendingDrains(t *testing.T) {
	rdb := newTestClient(t)
	ctx := context.Background()

	for _, off := range []int64{10, 11, 12} {
		if err := AddPending(ctx, rdb, "orders", "0", 2, off); err != nil {
			t.Fatalf("AddPending(%d): %v", off, err)
		}
	}

	min, ok, err := MinPending(ctx, rdb, "orders", "0", 2)
	if err != nil || !ok || min != 10 {
		t.Fatalf("MinPending = %d, ok=%v err=%v, want 10", min, ok, err)
	}

	if err := rdb.ZRem(ctx, PendingKey("orders", "0", 2), 10).Err(); err != nil {
		t.Fatalf("ack offset 10: %v", err)
	}
	min, ok, err = MinPending(ctx, rdb, "orders", "0", 2)
	if err != nil || !ok || min != 11 {
		t.Fatalf("MinPending after ack = %d, ok=%v err=%v, want 11", min, ok, err)
	}

	if err := SetWatermark(ctx, rdb, "orders", "0", 2, min-1); err != nil {
		t.Fatalf("SetWatermark: %v", err)
	}
	got, ok, err := GetWatermark(ctx, rdb, "orders", "0", 2)
	if err != nil || !ok || got != 10 {
		t.Fatalf("GetWatermark = %d, ok=%v err=%v, want 10", got, ok, err)
	}
}

func TestQueueMetaRoundTrip(t *testing.T) {
	rdb := newTestClient(t)
	ctx := context.Background()

	m := QueueMeta{FIFO: true, VisibilityTimeoutSeconds: 30, MaxReceiveCount: 5, PartitionsPerShard: 6, RetentionSeconds: 345600}
	if err := PutQueueMeta(ctx, rdb, "orders-fifo", m); err != nil {
		t.Fatalf("PutQueueMeta: %v", err)
	}
	got, ok, err := GetQueueMeta(ctx, rdb, "orders-fifo")
	if err != nil || !ok {
		t.Fatalf("GetQueueMeta: ok=%v err=%v", ok, err)
	}
	if got.FIFO != true || got.VisibilityTimeoutSeconds != 30 || got.MaxReceiveCount != 5 {
		t.Errorf("GetQueueMeta = %+v", got)
	}

	if err := DeleteQueueMeta(ctx, rdb, "orders-fifo"); err != nil {
		t.Fatalf("DeleteQueueMeta: %v", err)
	}
	_, ok, err = GetQueueMeta(ctx, rdb, "orders-fifo")
	if err != nil || ok {
		t.Fatalf("GetQueueMeta after delete: ok=%v err=%v, want false", ok, err)
	}
}

func TestShardMapRoundTrip(t *testing.T) {
	rdb := newTestClient(t)
	ctx := context.Background()

	shards := []kafka.Shard{
		{ID: "0", Topic: "kmsvc.orders.fifo.shard-0", HashRangeStart: 0, HashRangeEnd: kafka.FullHashRangeEnd},
	}
	if err := PutShardMap(ctx, rdb, "orders-fifo", shards); err != nil {
		t.Fatalf("PutShardMap: %v", err)
	}
	got, ok, err := GetShardMap(ctx, rdb, "orders-fifo")
	if err != nil || !ok {
		t.Fatalf("GetShardMap: ok=%v err=%v", ok, err)
	}
	if len(got) != 1 || got[0].Topic != shards[0].Topic {
		t.Errorf("GetShardMap = %+v, want %+v", got, shards)
	}

	if err := DeleteShardMap(ctx, rdb, "orders-fifo"); err != nil {
		t.Fatalf("DeleteShardMap: %v", err)
	}
	_, ok, err = GetShardMap(ctx, rdb, "orders-fifo")
	if err != nil || ok {
		t.Fatalf("GetShardMap after delete: ok=%v err=%v, want false", ok, err)
	}
}

func TestAckRemovesInFlightAndPending(t *testing.T) {
	rdb := newTestClient(t)
	ctx := context.Background()

	rec := InFlightRecord{ShardID: "0", Topic: "kmsvc.orders.shard-0", Partition: 1, Offset: 7, GroupID: "g1"}
	if err := PutInFlight(ctx, rdb, "orders", "rh-1", rec, 30*time.Second); err != nil {
		t.Fatalf("PutInFlight: %v", err)
	}
	if err := AddPending(ctx, rdb, "orders", "0", 1, 7); err != nil {
		t.Fatalf("AddPending: %v", err)
	}
	if _, err := AcquireFIFOLock(ctx, rdb, "orders", "g1", "rh-1", 30*time.Second); err != nil {
		t.Fatalf("AcquireFIFOLock: %v", err)
	}

	outcome, err := Ack(ctx, rdb, "orders", "rh-1")
	if err != nil || outcome != AckOutcomeAcked {
		t.Fatalf("Ack = %v, err=%v, want acked", outcome, err)
	}

	if _, ok, _ := GetInFlight(ctx, rdb, "orders", "rh-1"); ok {
		t.Error("in-flight record should be gone after ack")
	}
	if _, ok, _ := MinPending(ctx, rdb, "orders", "0", 1); ok {
		t.Error("pending offset should be removed after ack")
	}
	locked, err := AcquireFIFOLock(ctx, rdb, "orders", "g1", "rh-2", 30*time.Second)
	if err != nil || !locked {
		t.Errorf("fifo lock should be released after ack: locked=%v err=%v", locked, err)
	}

	// Acking again is a no-op, not an error.
	outcome, err = Ack(ctx, rdb, "orders", "rh-1")
	if err != nil || outcome != AckOutcomeNotFound {
		t.Fatalf("second Ack = %v, err=%v, want not_found", outcome, err)
	}
}

func TestReapRedeliversBelowMaxReceiveCount(t *testing.T) {
	rdb := newTestClient(t)
	ctx := context.Background()

	rec := InFlightRecord{ShardID: "0", Topic: "kmsvc.orders.shard-0", Partition: 1, Offset: 7, GroupID: "g1", ReceiveCount: 1, Body: "payload"}
	if err := PutInFlight(ctx, rdb, "orders", "rh-1", rec, time.Millisecond); err != nil {
		t.Fatalf("PutInFlight: %v", err)
	}
	if _, err := AcquireFIFOLock(ctx, rdb, "orders", "g1", "rh-1", 30*time.Second); err != nil {
		t.Fatalf("AcquireFIFOLock: %v", err)
	}

	res, err := Reap(ctx, rdb, "orders", "rh-1", 5)
	if err != nil {
		t.Fatalf("Reap: %v", err)
	}
	if res.Outcome != ReapOutcomeRedeliver || res.ReceiveCount != 2 {
		t.Fatalf("Reap = %+v, want redeliver with receiveCount=2", res)
	}

	handle, ok, err := PopRedeliverable(ctx, rdb, "orders")
	if err != nil || !ok || handle != "rh-1" {
		t.Fatalf("PopRedeliverable = %q, ok=%v err=%v, want rh-1", handle, ok, err)
	}

	got, ok, err := GetInFlight(ctx, rdb, "orders", "rh-1")
	if err != nil || !ok || got.ReceiveCount != 2 || got.Body != "payload" {
		t.Fatalf("GetInFlight after reap = %+v, ok=%v err=%v", got, ok, err)
	}

	relocked, err := AcquireFIFOLock(ctx, rdb, "orders", "g1", "rh-2", 30*time.Second)
	if err != nil || !relocked {
		t.Errorf("fifo lock should be released after reap-redeliver: ok=%v err=%v", relocked, err)
	}
}

func TestReapRoutesToDLQAtMaxReceiveCount(t *testing.T) {
	rdb := newTestClient(t)
	ctx := context.Background()

	rec := InFlightRecord{ShardID: "0", Topic: "kmsvc.orders.shard-0", Partition: 1, Offset: 7, GroupID: "g1", ReceiveCount: 5, Body: "payload"}
	if err := PutInFlight(ctx, rdb, "orders", "rh-1", rec, time.Millisecond); err != nil {
		t.Fatalf("PutInFlight: %v", err)
	}
	if err := AddPending(ctx, rdb, "orders", "0", 1, 7); err != nil {
		t.Fatalf("AddPending: %v", err)
	}

	res, err := Reap(ctx, rdb, "orders", "rh-1", 5)
	if err != nil {
		t.Fatalf("Reap: %v", err)
	}
	if res.Outcome != ReapOutcomeDLQ || res.Topic != rec.Topic || res.Offset != rec.Offset || res.Body != "payload" {
		t.Fatalf("Reap = %+v, want dlq with topic/offset/body matching the record", res)
	}

	if _, ok, _ := GetInFlight(ctx, rdb, "orders", "rh-1"); ok {
		t.Error("in-flight record should be gone after DLQ routing")
	}
	if _, ok, _ := MinPending(ctx, rdb, "orders", "0", 1); ok {
		t.Error("pending offset should be removed after DLQ routing")
	}
}

func TestReapConcurrentRaceHasExactlyOneWinner(t *testing.T) {
	rdb := newTestClient(t)
	ctx := context.Background()

	rec := InFlightRecord{ShardID: "0", Topic: "kmsvc.orders.shard-0", Partition: 1, Offset: 7, ReceiveCount: 4}
	if err := PutInFlight(ctx, rdb, "orders", "rh-1", rec, time.Millisecond); err != nil {
		t.Fatalf("PutInFlight: %v", err)
	}

	const n = 20
	var wins atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := Reap(ctx, rdb, "orders", "rh-1", 5)
			if err != nil {
				t.Errorf("Reap: %v", err)
				return
			}
			if res.Outcome != ReapOutcomeGone {
				wins.Add(1)
			}
		}()
	}
	wg.Wait()

	if got := wins.Load(); got != 1 {
		t.Errorf("expected exactly 1 non-gone outcome among %d concurrent reapers, got %d", n, got)
	}
}
