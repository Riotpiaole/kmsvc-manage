// Package operator implements the queue-operator control-plane reconciler
// described in design.md §2a/§2c: it turns a Queue CRD into one-or-more Kafka
// shard topics plus the Redis queue-metadata/shard-map the message-plane
// service reads on its hot path.
package operator

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"

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

	// Zones resolves shard topics' broker placement to availability zones
	// (design.md §2a AZ-awareness). Nil disables zone annotation entirely --
	// tests and any deployment without zone-labeled nodes can leave it unset.
	Zones *ZoneLocator

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
func (r *QueueReconciler) Reconcile(ctx context.Context, namespace, name string) error {
	var queue kmsvcv1.Queue
	err := r.Client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &queue)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("get queue %s/%s: %w", namespace, name, err)
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

	r.annotateAvailabilityZones(ctx, &queue)

	if err := r.reconcileSplits(ctx, &queue); err != nil {
		return r.setFailed(ctx, &queue, "ShardSplit", err)
	}

	if err := r.reconcileDrains(ctx, &queue); err != nil {
		return r.setFailed(ctx, &queue, "ShardDrain", err)
	}

	if err := r.publishRedisState(ctx, &queue); err != nil {
		return r.setFailed(ctx, &queue, "PublishRedis", err)
	}

	if err := r.reconcileTemporalWorker(ctx, &queue); err != nil {
		return r.setFailed(ctx, &queue, "TemporalWorker", err)
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
		HashRangeStart: int64(start),
		HashRangeEnd:   int64(end),
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

// annotateAvailabilityZones resolves and stamps each non-closed shard's
// AvailabilityZones (design.md §2a). Best-effort: a resolution failure for
// one shard is logged via setFailed-style swallowing -- it must never block
// the rest of reconciliation, since AZ info is status metadata, not
// load-bearing for the queue's actual operation.
func (r *QueueReconciler) annotateAvailabilityZones(ctx context.Context, queue *kmsvcv1.Queue) {
	if r.Zones == nil {
		return
	}
	logger := ctrllog.FromContext(ctx)
	for i := range queue.Status.Shards {
		s := &queue.Status.Shards[i]
		if s.Phase == kmsvcv1.ShardPhaseClosed {
			continue
		}
		brokerIDs, err := r.Admin.ReplicaBrokerIDs(ctx, s.Topic)
		if err != nil {
			logger.Error(err, "resolve replica broker IDs", "topic", s.Topic)
			continue
		}
		if len(brokerIDs) == 0 {
			logger.Info("no replica broker IDs returned", "topic", s.Topic)
			continue
		}
		zones, err := r.Zones.ZonesForBrokers(ctx, brokerIDs)
		if err != nil {
			logger.Error(err, "resolve zones for brokers", "topic", s.Topic, "brokerIDs", brokerIDs)
			continue
		}
		if len(zones) == 0 {
			logger.Info("no zones resolved", "topic", s.Topic, "brokerIDs", brokerIDs)
			continue
		}
		s.AvailabilityZones = zones
	}
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
			HashRangeStart: uint32(s.HashRangeStart),
			HashRangeEnd:   uint32(s.HashRangeEnd),
			Phase:          string(s.Phase),
		})
	}
	return kmsvcredis.PutShardMap(ctx, r.Redis, queue.Name, shards)
}

// reconcileTemporalWorker creates or updates a TemporalWorker CRD if the Queue
// has the temporal.io/namespace label. One TemporalWorker per Temporal namespace
// handles all task queues in that namespace (Kafka broker model).
func (r *QueueReconciler) reconcileTemporalWorker(ctx context.Context, queue *kmsvcv1.Queue) error {
	if queue.Labels == nil {
		return nil
	}

	namespace := queue.Labels["temporal.io/namespace"]
	if namespace == "" {
		return nil
	}

	if !isValidTemporalNamespace(namespace) {
		return fmt.Errorf("invalid temporal namespace label %q: must be lowercase alphanumeric and hyphens", namespace)
	}

	workerName := "worker-" + namespace
	if err := validateKubernetesName(workerName); err != nil {
		return fmt.Errorf("invalid kubernetes name %q: %w", workerName, err)
	}

	replicas := int32(1)
	workerNamespace := getEnvOrDefault("KMSVC_TEMPORAL_NAMESPACE", "temporal")
	workerImage := getEnvOrDefault("KMSVC_TEMPORAL_WORKER_IMAGE", "story-crater-backend:latest")

	worker := &kmsvcv1.TemporalWorker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      workerName,
			Namespace: workerNamespace,
		},
	}

	// Queue and TemporalWorker live in different namespaces (sqs vs. the Temporal
	// namespace), so a controller owner reference is disallowed by the API server.
	// Lifecycle is instead managed explicitly in reconcileDelete.
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, worker, func() error {
		worker.Spec.Namespace = namespace
		worker.Spec.Image = workerImage
		worker.Spec.Replicas = &replicas
		return nil
	}); err != nil {
		return fmt.Errorf("create or update TemporalWorker %s: %w", workerName, err)
	}
	return nil
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

	if queue.Labels != nil {
		namespace := queue.Labels["temporal.io/namespace"]
		if namespace != "" {
			workerName := "worker-" + namespace
			workerNamespace := getEnvOrDefault("KMSVC_TEMPORAL_NAMESPACE", "temporal")
			worker := &kmsvcv1.TemporalWorker{
				ObjectMeta: metav1.ObjectMeta{
					Name:      workerName,
					Namespace: workerNamespace,
				},
			}
			if err := r.Client.Delete(ctx, worker); err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf("delete TemporalWorker %s: %w", workerName, err)
			}
		}
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

func isValidTemporalNamespace(namespace string) bool {
	if len(namespace) == 0 || len(namespace) > 255 {
		return false
	}
	for _, ch := range namespace {
		if !((ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '_' || ch == '-') {
			return false
		}
	}
	return true
}

func validateKubernetesName(name string) error {
	if len(name) == 0 || len(name) > 253 {
		return fmt.Errorf("name length must be 1-253 characters")
	}
	for i, ch := range name {
		isLower := ch >= 'a' && ch <= 'z'
		isDigit := ch >= '0' && ch <= '9'
		isHyphen := ch == '-'
		isValid := isLower || isDigit || isHyphen
		if !isValid {
			return fmt.Errorf("name contains invalid character %q at position %d", ch, i)
		}
		if i == 0 && (isHyphen || isDigit) {
			return fmt.Errorf("name must start with lowercase letter")
		}
		if i == len(name)-1 && isHyphen {
			return fmt.Errorf("name must end with lowercase letter or digit")
		}
	}
	return nil
}

func getEnvOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
