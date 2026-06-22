// Package reaper implements the goroutine-based redelivery/DLQ sweep from
// design.md §5: a per-queue ticker scans vis_index for expired in-flight
// messages and drives each through the atomic reap.lua check-and-act.
package reaper

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/rockliang/kafka-management-service/internal/core/queue"
	kmsvcredis "github.com/rockliang/kafka-management-service/internal/redis"
)

const (
	defaultSweepInterval = 5 * time.Second
	sweepBatchSize       = 100
)

// DLQSender is the subset of queue.SendMessageService a Reaper needs to
// route a maxed-out message to its queue's dead-letter target. Abstracted so
// tests can substitute a fake without a real Kafka broker.
type DLQSender interface {
	SendMessage(ctx context.Context, in queue.SendMessageInput) (queue.SendMessageOutput, error)
}

// Reaper sweeps one or more queues' vis_index ZSETs for expired in-flight
// messages. Safe to run concurrently from multiple replicas against the same
// queue: reap.lua's check-and-act is atomic per receipt handle, so only the
// first caller for a given expired entry gets a non-"gone" outcome.
type Reaper struct {
	Redis     *goredis.Client
	DLQSender DLQSender

	// Interval is the sleep between sweep ticks for Run; defaults to 5s.
	Interval time.Duration
}

// Sweep runs one pass over queueName's vis_index, returning the number of
// expired entries it took action on (redelivered or DLQ-routed; entries
// another replica already won the race for don't count).
func (r *Reaper) Sweep(ctx context.Context, queueName string) (int, error) {
	meta, ok, err := kmsvcredis.GetQueueMeta(ctx, r.Redis, queueName)
	if err != nil {
		return 0, fmt.Errorf("reaper sweep %s: %w", queueName, err)
	}
	if !ok {
		return 0, nil
	}

	handles, err := kmsvcredis.ExpiredVisIndexEntries(ctx, r.Redis, queueName, sweepBatchSize)
	if err != nil {
		return 0, fmt.Errorf("reaper sweep %s: %w", queueName, err)
	}

	swept := 0
	for _, handle := range handles {
		result, err := kmsvcredis.Reap(ctx, r.Redis, queueName, handle, meta.MaxReceiveCount)
		if err != nil {
			return swept, fmt.Errorf("reaper sweep %s: %w", queueName, err)
		}

		switch result.Outcome {
		case kmsvcredis.ReapOutcomeGone:
			continue
		case kmsvcredis.ReapOutcomeDLQ:
			if err := r.routeToDLQ(ctx, meta, result); err != nil {
				return swept, fmt.Errorf("reaper sweep %s: %w", queueName, err)
			}
			swept++
		case kmsvcredis.ReapOutcomeRedeliver:
			swept++
		}
	}
	return swept, nil
}

// routeToDLQ produces a maxed-out message onto its queue's configured
// dead-letter queue. A queue with no DLQ configured just drops the message
// here, matching reap.lua having already deleted all of its state.
func (r *Reaper) routeToDLQ(ctx context.Context, meta kmsvcredis.QueueMeta, result kmsvcredis.ReapResult) error {
	if meta.DLQQueueName == "" || r.DLQSender == nil {
		return nil
	}
	_, err := r.DLQSender.SendMessage(ctx, queue.SendMessageInput{
		QueueName:      meta.DLQQueueName,
		Body:           result.Body,
		MessageGroupID: result.GroupID,
	})
	return err
}

// Run sweeps queueName on a ticker until ctx is done. Intended to be started
// as its own goroutine per queue (design.md §5); sweep errors are logged-ish
// via the returned channel-free design (transient Redis/Kafka hiccups are
// retried on the next tick rather than stopping the loop).
func (r *Reaper) Run(ctx context.Context, queueName string, onSweepError func(error)) {
	interval := r.Interval
	if interval <= 0 {
		interval = defaultSweepInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := r.Sweep(ctx, queueName); err != nil && onSweepError != nil {
				onSweepError(err)
			}
		}
	}
}
