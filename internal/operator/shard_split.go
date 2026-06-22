package operator

import (
	"context"
	"fmt"
	"strconv"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kmsvcv1 "github.com/rockliang/kafka-management-service/apis/kmsvc/v1"
	"github.com/rockliang/kafka-management-service/internal/kafka"
)

// reconcileSplits samples each Active shard's throughput and splits any shard
// that's sustained over spec.ShardSplitThresholdBytesPerSec into two children,
// per design.md §2c.
func (r *QueueReconciler) reconcileSplits(ctx context.Context, queue *kmsvcv1.Queue) error {
	if r.sampleState == nil {
		r.sampleState = map[string]sample{}
	}

	maxShards := queue.Spec.MaxShards
	if maxShards <= 0 {
		maxShards = 8
	}
	threshold := queue.Spec.ShardSplitThresholdBytesPerSec
	if threshold <= 0 {
		return nil
	}
	cooldown := queue.Spec.ShardSplitCooldownSeconds

	activeCount := 0
	for _, s := range queue.Status.Shards {
		if s.Phase == kmsvcv1.ShardPhaseActive {
			activeCount++
		}
	}

	for i := range queue.Status.Shards {
		s := &queue.Status.Shards[i]
		if s.Phase != kmsvcv1.ShardPhaseActive {
			continue
		}
		if int32(activeCount) >= maxShards {
			break
		}
		if !s.CreatedAt.IsZero() && r.now().Before(s.CreatedAt.Add(secondsToDuration(cooldown))) {
			continue
		}

		offsetSum, err := r.Admin.LogEndOffsetSum(ctx, s.Topic)
		if err != nil {
			return fmt.Errorf("sample throughput %s: %w", s.Topic, err)
		}
		prev, seen := r.sampleState[s.Topic]
		now := r.now()
		r.sampleState[s.Topic] = sample{offsetSum: offsetSum, at: now}
		if !seen {
			continue
		}
		elapsed := now.Sub(prev.at).Seconds()
		if elapsed <= 0 {
			continue
		}
		recordsPerSec := float64(offsetSum-prev.offsetSum) / elapsed
		bytesPerSec := recordsPerSec * avgMessageSizeBytesEstimate
		if bytesPerSec < float64(threshold) {
			continue
		}

		mid := kafka.SplitHashRange(uint32(s.HashRangeStart), uint32(s.HashRangeEnd))
		childAID := nextShardID(queue.Status.Shards)
		childA := r.newShard(childAID, s.ID, uint32(s.HashRangeStart), mid, queue)
		childA.CreatedAt = metav1.NewTime(now)
		childAIDNum, _ := strconv.Atoi(childAID)
		childBID := strconv.Itoa(childAIDNum + 1)
		childB := r.newShard(childBID, s.ID, mid, uint32(s.HashRangeEnd), queue)
		childB.CreatedAt = metav1.NewTime(now)

		s.Phase = kmsvcv1.ShardPhaseClosing
		queue.Status.Shards = append(queue.Status.Shards, childA, childB)
		activeCount += 1 // net: -1 parent +2 children
		delete(r.sampleState, s.Topic)
	}
	return nil
}
