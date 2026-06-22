package operator

import (
	"context"
	"fmt"
	"time"

	kmsvcv1 "github.com/rockliang/kafka-management-service/apis/kmsvc/v1"
	"github.com/rockliang/kafka-management-service/internal/kafka"
)

func secondsToDuration(s int32) time.Duration {
	return time.Duration(s) * time.Second
}

// reconcileDrains transitions `Closing` shards to `Closed` once their
// consumer-group lag reaches zero and the retention window has elapsed since
// they stopped being a write target, then deletes their topic, per
// design.md §2c.
func (r *QueueReconciler) reconcileDrains(ctx context.Context, queue *kmsvcv1.Queue) error {
	group := kafka.ConsumerGroup(queue.Name)
	retention := secondsToDuration(queue.Spec.MessageRetentionPeriodSeconds)

	kept := make([]kmsvcv1.ShardStatus, 0, len(queue.Status.Shards))
	for _, s := range queue.Status.Shards {
		if s.Phase != kmsvcv1.ShardPhaseClosing {
			kept = append(kept, s)
			continue
		}

		lag, err := r.Admin.ConsumerLag(ctx, group, s.Topic)
		if err != nil {
			return fmt.Errorf("consumer lag %s: %w", s.Topic, err)
		}
		if lag > 0 || r.now().Before(s.CreatedAt.Add(retention)) {
			kept = append(kept, s)
			continue
		}

		if err := r.Admin.DeleteTopic(ctx, s.Topic); err != nil {
			return fmt.Errorf("delete drained topic %s: %w", s.Topic, err)
		}
		s.Phase = kmsvcv1.ShardPhaseClosed
		kept = append(kept, s)
	}
	queue.Status.Shards = kept
	return nil
}
