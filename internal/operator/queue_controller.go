// Package operator implements the queue-operator control-plane reconciler
// described in design.md §2a/§2c: it turns a Queue CRD into one-or-more Kafka
// shard topics plus the Redis queue-metadata/shard-map the message-plane
// service reads on its hot path.
package operator

import (
	"context"
	"fmt"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	kmsvcv1 "github.com/rockliang/kafka-management-service/apis/kmsvc/v1"
	"github.com/rockliang/kafka-management-service/internal/kafka"
	kmsvcredis "github.com/rockliang/kafka-management-service/internal/redis"
)

const finalizerName = "kmsvc.io/queue-operator"

// avgMessageSizeBytesEstimate converts the record-rate sampled from Kafka
// end-offset deltas into an approximate bytes/sec figure to compare against
// ShardSplitThresholdBytesPerSec, since there's no metrics pipeline (e.g.
// Prometheus) wired up in v1 to get a real byte rate per design.md §2c.
const avgMessageSizeBytesEstimate = 1024

const replicationFactor = 3
const minInsyncReplicas = 2

// QueueReconciler reconciles Queue objects (design.md §2a).
type QueueReconciler struct {
	Client client.Client
	Admin  TopicAdmin
	Redis  *goredis.Client
	Now    func() time.Time

	// sampleState tracks the last (offsetSum, time) seen per shard topic, used
	// to compute a throughput estimate between reconciles. Keyed by topic name.
	sampleState map[string]sample
}

type sample struct {
	offsetSum int64
	at        time.Time
}

func (r *QueueReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// Reconcile implements the controller-runtime reconcile loop.
func (r *QueueReconciler) Reconcile(ctx context.Context, name string) error {
	var queue kmsvcv1.Queue
	err := r.Client.Get(ctx, client.ObjectKey{Name: name}, &queue)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("get queue %s: %w", name, err)
	}

	if !queue.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, &queue)
	}

	if err := kafka.ValidateNoDLQCycle(queue.Name, queue.Spec.IsDLQ, queue.Spec.DeadLetterTargetQueue); err != nil {
		return r.setFailed(ctx, &queue, "DLQCycle", err)
	}

	if !controllerutil.ContainsFinalizer(&queue, finalizerName) {
		controllerutil.AddFinalizer(&queue, finalizerName)
		if err := r.Client.Update(ctx, &queue); err != nil {
			return fmt.Errorf("add finalizer %s: %w", name, err)
		}
	}

	if len(queue.Status.Shards) == 0 {
		queue.Status.Shards = []kmsvcv1.ShardStatus{r.newShard("0", "", 0, kafka.FullHashRangeEnd, &queue)}
	}

	if err := r.ensureShardTopics(ctx, &queue); err != nil {
		return r.setFailed(ctx, &queue, "EnsureTopics", err)
	}

	if err := r.reconcileSplits(ctx, &queue); err != nil {
		return r.setFailed(ctx, &queue, "ShardSplit", err)
	}

	if err := r.reconcileDrains(ctx, &queue); err != nil {
		return r.setFailed(ctx, &queue, "ShardDrain", err)
	}

	if err := r.publishRedisState(ctx, &queue); err != nil {
		return r.setFailed(ctx, &queue, "PublishRedis", err)
	}

	queue.Status.Phase = kmsvcv1.QueuePhaseReady
	if err := r.Client.Status().Update(ctx, &queue); err != nil {
		return fmt.Errorf("update status %s: %w", name, err)
	}
	return nil
}

func (r *QueueReconciler) newShard(id, parentID string, start, end uint32, queue *kmsvcv1.Queue) kmsvcv1.ShardStatus {
	return kmsvcv1.ShardStatus{
		ID:             id,
		Topic:          kafka.ShardTopicName(queue.Name, queue.Spec.FIFOQueue, id),
		HashRangeStart: start,
		HashRangeEnd:   end,
		Phase:          kmsvcv1.ShardPhaseActive,
		ParentID:       parentID,
		CreatedAt:      metav1.NewTime(r.now()),
	}
}

func (r *QueueReconciler) ensureShardTopics(ctx context.Context, queue *kmsvcv1.Queue) error {
	cfg := kafka.TopicConfig{
		PartitionCount:    queue.Spec.PartitionsPerShard,
		ReplicationFactor: replicationFactor,
		RetentionSeconds:  queue.Spec.MessageRetentionPeriodSeconds,
		MinInsyncReplicas: minInsyncReplicas,
	}
	for _, s := range queue.Status.Shards {
		if s.Phase == kmsvcv1.ShardPhaseClosed {
			continue
		}
		if err := r.Admin.CreateTopic(ctx, s.Topic, cfg); err != nil {
			return fmt.Errorf("ensure topic %s: %w", s.Topic, err)
		}
	}
	return nil
}

func (r *QueueReconciler) setFailed(ctx context.Context, queue *kmsvcv1.Queue, reason string, cause error) error {
	queue.Status.Phase = kmsvcv1.QueuePhaseFailed
	if err := r.Client.Status().Update(ctx, queue); err != nil {
		return fmt.Errorf("update failed status %s (reason=%s, cause=%v): %w", queue.Name, reason, cause, err)
	}
	return fmt.Errorf("reconcile %s failed (%s): %w", queue.Name, reason, cause)
}

func (r *QueueReconciler) publishRedisState(ctx context.Context, queue *kmsvcv1.Queue) error {
	if err := kmsvcredis.PutQueueMeta(ctx, r.Redis, queue.Name, kmsvcredis.QueueMeta{
		FIFO:                     queue.Spec.FIFOQueue,
		VisibilityTimeoutSeconds: queue.Spec.VisibilityTimeoutSeconds,
		MaxReceiveCount:          queue.Spec.MaxReceiveCount,
		DLQQueueName:             queue.Spec.DeadLetterTargetQueue,
		PartitionsPerShard:       queue.Spec.PartitionsPerShard,
		RetentionSeconds:         queue.Spec.MessageRetentionPeriodSeconds,
		CreatedAt:                queue.CreationTimestamp.Time,
	}); err != nil {
		return err
	}

	shards := make([]kafka.Shard, 0, len(queue.Status.Shards))
	for _, s := range queue.Status.Shards {
		if s.Phase == kmsvcv1.ShardPhaseClosed {
			continue
		}
		shards = append(shards, kafka.Shard{
			ID:             s.ID,
			Topic:          s.Topic,
			HashRangeStart: s.HashRangeStart,
			HashRangeEnd:   s.HashRangeEnd,
			Phase:          string(s.Phase),
		})
	}
	return kmsvcredis.PutShardMap(ctx, r.Redis, queue.Name, shards)
}

func (r *QueueReconciler) reconcileDelete(ctx context.Context, queue *kmsvcv1.Queue) error {
	if !controllerutil.ContainsFinalizer(queue, finalizerName) {
		return nil
	}
	for _, s := range queue.Status.Shards {
		if s.Phase == kmsvcv1.ShardPhaseClosed {
			continue
		}
		if err := r.Admin.DeleteTopic(ctx, s.Topic); err != nil {
			return fmt.Errorf("delete topic %s: %w", s.Topic, err)
		}
	}
	if err := kmsvcredis.DeleteQueueMeta(ctx, r.Redis, queue.Name); err != nil {
		return err
	}
	if err := kmsvcredis.DeleteShardMap(ctx, r.Redis, queue.Name); err != nil {
		return err
	}
	controllerutil.RemoveFinalizer(queue, finalizerName)
	if err := r.Client.Update(ctx, queue); err != nil {
		return fmt.Errorf("remove finalizer %s: %w", queue.Name, err)
	}
	return nil
}

func nextShardID(shards []kmsvcv1.ShardStatus) string {
	max := -1
	for _, s := range shards {
		if n, err := strconv.Atoi(s.ID); err == nil && n > max {
			max = n
		}
	}
	return strconv.Itoa(max + 1)
}
