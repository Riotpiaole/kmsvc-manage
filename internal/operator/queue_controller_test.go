package operator

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	kmsvcv1 "github.com/rockliang/kafka-management-service/apis/kmsvc/v1"
	"github.com/rockliang/kafka-management-service/internal/kafka"
	kmsvcredis "github.com/rockliang/kafka-management-service/internal/redis"
)

func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := kmsvcv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add kmsvc scheme: %v", err)
	}
	return scheme
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

func newTestSchemeWithAppsV1(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := newTestScheme(t)
	appsv1 := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(appsv1); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	return scheme
}

func newTestReconciler(t *testing.T, objs ...client.Object) (*QueueReconciler, *fakeAdmin) {
	t.Helper()
	r, admin, _ := newTestReconcilerWithTemporal(t, objs...)
	return r, admin
}

func newTestReconcilerWithTemporal(t *testing.T, objs ...client.Object) (*QueueReconciler, *fakeAdmin, *fakeTemporal) {
	t.Helper()
	scheme := newTestScheme(t)
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&kmsvcv1.Queue{}).
		WithObjects(objs...).
		Build()
	admin := newFakeAdmin()
	temporal := newFakeTemporal()
	return &QueueReconciler{
		Client:   cl,
		Admin:    admin,
		Redis:    newTestRedis(t),
		Now:      time.Now,
		Temporal: temporal,
	}, admin, temporal
}

func baseQueue(name string) *kmsvcv1.Queue {
	return &kmsvcv1.Queue{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: kmsvcv1.QueueSpec{
			VisibilityTimeoutSeconds:      30,
			MessageRetentionPeriodSeconds: 345600,
			MaxReceiveCount:               5,
			PartitionsPerShard:            6,
			MinShards:                     1,
			MaxShards:                     8,
		},
	}
}

func TestReconcileCreatesShardZeroAndPublishesRedisState(t *testing.T) {
	queue := baseQueue("orders")
	r, admin := newTestReconciler(t, queue)
	ctx := context.Background()

	if err := r.Reconcile(ctx, "", "orders"); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	var got kmsvcv1.Queue
	if err := r.Client.Get(ctx, client.ObjectKey{Name: "orders"}, &got); err != nil {
		t.Fatalf("get queue: %v", err)
	}
	if got.Status.Phase != kmsvcv1.QueuePhaseReady {
		t.Fatalf("phase = %v, want Ready", got.Status.Phase)
	}
	if len(got.Status.Shards) != 1 || got.Status.Shards[0].ID != "0" {
		t.Fatalf("shards = %+v, want one shard-0", got.Status.Shards)
	}
	wantTopic := kafka.ShardTopicName("orders", false, "0")
	if got.Status.Shards[0].Topic != wantTopic {
		t.Errorf("shard-0 topic = %q, want %q", got.Status.Shards[0].Topic, wantTopic)
	}
	if !admin.hasTopic(wantTopic) {
		t.Errorf("expected topic %q to be created", wantTopic)
	}

	meta, ok, err := kmsvcredis.GetQueueMeta(ctx, r.Redis, "orders")
	if err != nil || !ok {
		t.Fatalf("GetQueueMeta: ok=%v err=%v", ok, err)
	}
	if meta.MaxReceiveCount != 5 {
		t.Errorf("meta.MaxReceiveCount = %d, want 5", meta.MaxReceiveCount)
	}

	shards, ok, err := kmsvcredis.GetShardMap(ctx, r.Redis, "orders")
	if err != nil || !ok || len(shards) != 1 {
		t.Fatalf("GetShardMap: shards=%+v ok=%v err=%v", shards, ok, err)
	}
}

func TestReconcileRejectsDLQSelfReference(t *testing.T) {
	queue := baseQueue("orders")
	queue.Spec.DeadLetterTargetQueue = "orders"
	r, _ := newTestReconciler(t, queue)
	ctx := context.Background()

	err := r.Reconcile(ctx, "", "orders")
	if err == nil {
		t.Fatal("expected reconcile to fail for self-referencing DLQ")
	}

	var got kmsvcv1.Queue
	if err := r.Client.Get(ctx, client.ObjectKey{Name: "orders"}, &got); err != nil {
		t.Fatalf("get queue: %v", err)
	}
	if got.Status.Phase != kmsvcv1.QueuePhaseFailed {
		t.Errorf("phase = %v, want Failed", got.Status.Phase)
	}
}

func TestReconcileDeleteCleansUpTopicsAndRedis(t *testing.T) {
	queue := baseQueue("orders")
	r, admin := newTestReconciler(t, queue)
	ctx := context.Background()

	if err := r.Reconcile(ctx, "", "orders"); err != nil {
		t.Fatalf("initial reconcile: %v", err)
	}

	var got kmsvcv1.Queue
	if err := r.Client.Get(ctx, client.ObjectKey{Name: "orders"}, &got); err != nil {
		t.Fatalf("get queue: %v", err)
	}
	topic := got.Status.Shards[0].Topic

	if err := r.Client.Delete(ctx, &got); err != nil {
		t.Fatalf("delete queue: %v", err)
	}
	if err := r.Reconcile(ctx, "", "orders"); err != nil {
		t.Fatalf("delete reconcile: %v", err)
	}

	if admin.hasTopic(topic) {
		t.Errorf("expected topic %q to be deleted", topic)
	}
	if _, ok, _ := kmsvcredis.GetQueueMeta(ctx, r.Redis, "orders"); ok {
		t.Error("expected queue meta to be removed")
	}
	if _, ok, _ := kmsvcredis.GetShardMap(ctx, r.Redis, "orders"); ok {
		t.Error("expected shard map to be removed")
	}
}

func TestReconcileSplitsShardOverThreshold(t *testing.T) {
	queue := baseQueue("orders")
	queue.Spec.ShardSplitThresholdBytesPerSec = 1000
	queue.Spec.ShardSplitCooldownSeconds = 0
	now := time.Now()
	r, admin := newTestReconciler(t, queue)
	r.Now = func() time.Time { return now }
	ctx := context.Background()

	if err := r.Reconcile(ctx, "", "orders"); err != nil {
		t.Fatalf("initial reconcile: %v", err)
	}
	var got kmsvcv1.Queue
	if err := r.Client.Get(ctx, client.ObjectKey{Name: "orders"}, &got); err != nil {
		t.Fatalf("get queue: %v", err)
	}
	shard0Topic := got.Status.Shards[0].Topic
	admin.setOffsetSum(shard0Topic, 0)

	// First sample establishes the baseline (no delta to compare yet).
	if err := r.Reconcile(ctx, "", "orders"); err != nil {
		t.Fatalf("baseline reconcile: %v", err)
	}

	// Advance time and offsets enough to exceed 1000 bytes/sec at the
	// 1024-byte/record estimate: 10 records over 1s ≈ 10240 bytes/sec.
	now = now.Add(time.Second)
	admin.setOffsetSum(shard0Topic, 10)

	if err := r.Reconcile(ctx, "", "orders"); err != nil {
		t.Fatalf("split-triggering reconcile: %v", err)
	}

	if err := r.Client.Get(ctx, client.ObjectKey{Name: "orders"}, &got); err != nil {
		t.Fatalf("get queue after split: %v", err)
	}
	if len(got.Status.Shards) != 3 {
		t.Fatalf("shards = %+v, want 3 (1 closing parent + 2 active children)", got.Status.Shards)
	}
	var parent *kmsvcv1.ShardStatus
	var children []kmsvcv1.ShardStatus
	for i := range got.Status.Shards {
		s := &got.Status.Shards[i]
		if s.ID == "0" {
			parent = s
		} else {
			children = append(children, *s)
		}
	}
	if parent == nil || parent.Phase != kmsvcv1.ShardPhaseClosing {
		t.Fatalf("parent shard = %+v, want Closing", parent)
	}
	if len(children) != 2 {
		t.Fatalf("children = %+v, want 2", children)
	}
	mid := kafka.SplitHashRange(uint32(parent.HashRangeStart), uint32(parent.HashRangeEnd))
	gotRanges := map[[2]uint32]bool{}
	for _, c := range children {
		if c.Phase != kmsvcv1.ShardPhaseActive {
			t.Errorf("child %s phase = %v, want Active", c.ID, c.Phase)
		}
		gotRanges[[2]uint32{uint32(c.HashRangeStart), uint32(c.HashRangeEnd)}] = true
	}
	if !gotRanges[[2]uint32{0, mid}] || !gotRanges[[2]uint32{mid, kafka.FullHashRangeEnd}] {
		t.Errorf("child ranges = %+v, want [0,%d) and [%d,%d)", children, mid, mid, kafka.FullHashRangeEnd)
	}
}

func TestReconcileDrainsClosingShardWhenLagZero(t *testing.T) {
	queue := baseQueue("orders")
	queue.Spec.MessageRetentionPeriodSeconds = 60
	now := time.Now()
	r, admin := newTestReconciler(t, queue)
	r.Now = func() time.Time { return now }
	ctx := context.Background()

	if err := r.Reconcile(ctx, "", "orders"); err != nil {
		t.Fatalf("initial reconcile: %v", err)
	}
	var got kmsvcv1.Queue
	if err := r.Client.Get(ctx, client.ObjectKey{Name: "orders"}, &got); err != nil {
		t.Fatalf("get queue: %v", err)
	}

	// Manually mark shard-0 as Closing (simulating a prior split) to isolate
	// drain behavior from split behavior.
	got.Status.Shards[0].Phase = kmsvcv1.ShardPhaseClosing
	got.Status.Shards[0].CreatedAt = metav1.NewTime(now.Add(-2 * time.Minute))
	if err := r.Client.Status().Update(ctx, &got); err != nil {
		t.Fatalf("seed closing shard: %v", err)
	}
	topic := got.Status.Shards[0].Topic
	admin.setLag(kafka.ConsumerGroup("orders"), topic, 0)

	if err := r.Reconcile(ctx, "", "orders"); err != nil {
		t.Fatalf("drain reconcile: %v", err)
	}

	if err := r.Client.Get(ctx, client.ObjectKey{Name: "orders"}, &got); err != nil {
		t.Fatalf("get queue after drain: %v", err)
	}
	if got.Status.Shards[0].Phase != kmsvcv1.ShardPhaseClosed {
		t.Errorf("shard-0 phase = %v, want Closed", got.Status.Shards[0].Phase)
	}
	if admin.hasTopic(topic) {
		t.Errorf("expected drained topic %q to be deleted", topic)
	}
}

func TestReconcileTemporalWorkerCreatesWhenLabelPresent(t *testing.T) {
	queue := baseQueue("orders")
	queue.Labels = map[string]string{"temporal.io/namespace": "default"}
	r, _, temporal := newTestReconcilerWithTemporal(t, queue)
	ctx := context.Background()

	if err := r.Reconcile(ctx, "", "orders"); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	var worker kmsvcv1.TemporalWorker
	if err := r.Client.Get(ctx, client.ObjectKey{Name: "worker-default", Namespace: "temporal"}, &worker); err != nil {
		t.Fatalf("expected TemporalWorker to be created: %v", err)
	}
	if worker.Spec.Namespace != "default" {
		t.Errorf("worker namespace = %q, want default", worker.Spec.Namespace)
	}
	if got := temporal.count("default"); got != 1 {
		t.Errorf("RegisterNamespace(%q) called %d times, want 1", "default", got)
	}
}

func TestReconcileTemporalWorkerRegistersNamespaceBeforeCreating(t *testing.T) {
	queue := baseQueue("orders")
	queue.Labels = map[string]string{"temporal.io/namespace": "checkout"}
	r, _, temporal := newTestReconcilerWithTemporal(t, queue)
	ctx := context.Background()

	if err := r.Reconcile(ctx, "", "orders"); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := temporal.count("checkout"); got != 1 {
		t.Errorf("RegisterNamespace(%q) called %d times, want 1", "checkout", got)
	}
}

func TestReconcileTemporalWorkerFailsWhenNamespaceRegistrationFails(t *testing.T) {
	queue := baseQueue("orders")
	queue.Labels = map[string]string{"temporal.io/namespace": "checkout"}
	r, _, temporal := newTestReconcilerWithTemporal(t, queue)
	temporal.setErr(fmt.Errorf("frontend unreachable"))
	ctx := context.Background()

	err := r.Reconcile(ctx, "", "orders")
	if err == nil {
		t.Fatal("expected Reconcile to fail when namespace registration fails")
	}

	var worker kmsvcv1.TemporalWorker
	getErr := r.Client.Get(ctx, client.ObjectKey{Name: "worker-checkout", Namespace: "temporal"}, &worker)
	if getErr == nil {
		t.Error("expected no TemporalWorker to be created when namespace registration fails")
	}
}

func TestReconcileTemporalWorkerValidatesNamespaceLabel(t *testing.T) {
	queue := baseQueue("orders")
	queue.Labels = map[string]string{"temporal.io/namespace": "Foo@Bar"}
	r, _ := newTestReconciler(t, queue)
	ctx := context.Background()

	err := r.Reconcile(ctx, "", "orders")
	if err == nil {
		t.Fatal("expected Reconcile to fail with invalid namespace label")
	}
	if err.Error() == "" || err.Error() == "invalid temporal namespace" {
		t.Errorf("error message not descriptive: %v", err)
	}
}

func TestReconcileTemporalWorkerValidatesKubernetesName(t *testing.T) {
	queue := baseQueue("orders")
	queue.Labels = map[string]string{"temporal.io/namespace": strings.Repeat("a", 250)}
	r, _ := newTestReconciler(t, queue)
	ctx := context.Background()

	err := r.Reconcile(ctx, "", "orders")
	if err == nil {
		t.Fatal("expected Reconcile to fail with too-long kubernetes name")
	}
}

func TestReconcileDeleteRemovesTemporalWorker(t *testing.T) {
	queue := baseQueue("orders")
	queue.Labels = map[string]string{"temporal.io/namespace": "default"}
	r, _ := newTestReconciler(t, queue)
	ctx := context.Background()

	if err := r.Reconcile(ctx, "", "orders"); err != nil {
		t.Fatalf("initial reconcile: %v", err)
	}

	var got kmsvcv1.Queue
	if err := r.Client.Get(ctx, client.ObjectKey{Name: "orders"}, &got); err != nil {
		t.Fatalf("get queue: %v", err)
	}

	if err := r.Client.Delete(ctx, &got); err != nil {
		t.Fatalf("delete queue: %v", err)
	}
	if err := r.Reconcile(ctx, "", "orders"); err != nil {
		t.Fatalf("delete reconcile: %v", err)
	}

	var worker kmsvcv1.TemporalWorker
	err := r.Client.Get(ctx, client.ObjectKey{Name: "worker-default", Namespace: "temporal"}, &worker)
	if err == nil {
		t.Error("expected TemporalWorker to be deleted")
	}
}

func TestIsValidTemporalNamespace(t *testing.T) {
	tests := []struct {
		name string
		ns   string
		want bool
	}{
		{"valid lowercase", "default", true},
		{"valid with underscore", "my_namespace", true},
		{"valid with hyphen", "my-namespace", true},
		{"valid with digits", "ns123", true},
		{"invalid uppercase", "MyNamespace", false},
		{"invalid special chars", "my@namespace", false},
		{"empty", "", false},
		{"too long", strings.Repeat("a", 256), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isValidTemporalNamespace(tt.ns); got != tt.want {
				t.Errorf("isValidTemporalNamespace(%q) = %v, want %v", tt.ns, got, tt.want)
			}
		})
	}
}

func TestValidateKubernetesName(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		wantOk bool
	}{
		{"valid", "worker-default", true},
		{"valid lowercase digits", "worker-123", true},
		{"invalid uppercase", "Worker-default", false},
		{"invalid starts with hyphen", "-worker-default", false},
		{"invalid ends with hyphen", "worker-default-", false},
		{"invalid special char", "worker@default", false},
		{"too long", "worker-" + strings.Repeat("a", 250), false},
		{"starts with digit", "1worker-default", false},
		{"empty", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateKubernetesName(tt.input)
			got := err == nil
			if got != tt.wantOk {
				t.Errorf("validateKubernetesName(%q) error = %v, want ok=%v", tt.input, err, tt.wantOk)
			}
		})
	}
}
