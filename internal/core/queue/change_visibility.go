package queue

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	kmsvcredis "github.com/rockliang/kafka-management-service/internal/redis"
)

type ChangeVisibilityService struct {
	Redis *goredis.Client
}

// ChangeMessageVisibility implements ChangeMessageVisibility (design.md
// §2b): extends (or shortens) how long a received message stays invisible
// to other consumers, without affecting its receive count.
func (s *ChangeVisibilityService) ChangeMessageVisibility(ctx context.Context, queueName, receiptHandle string, newTimeout time.Duration) error {
	ok, err := kmsvcredis.ExtendVisibility(ctx, s.Redis, queueName, receiptHandle, newTimeout)
	if err != nil {
		return fmt.Errorf("change visibility %s: %w", queueName, err)
	}
	if !ok {
		return fmt.Errorf("receipt handle not found or already expired")
	}
	return nil
}
