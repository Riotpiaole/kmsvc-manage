// Package redis implements the key schemas and atomic Lua scripts from
// design.md §4/§5, shared by the queue-operator and the message-plane service.
package redis

import "fmt"

func InFlightKey(queue, receiptHandle string) string {
	return fmt.Sprintf("kmsvc:inflight:%s:%s", queue, receiptHandle)
}

func PendingKey(queue, shardID string, partition int32) string {
	return fmt.Sprintf("kmsvc:pending:%s:%s:%d", queue, shardID, partition)
}

func WatermarkKey(queue, shardID string, partition int32) string {
	return fmt.Sprintf("kmsvc:watermark:%s:%s:%d", queue, shardID, partition)
}

func VisIndexKey(queue string) string {
	return fmt.Sprintf("kmsvc:vis_index:%s", queue)
}

func DedupKey(queue, groupID, dedupID string) string {
	return fmt.Sprintf("kmsvc:dedup:%s:%s:%s", queue, groupID, dedupID)
}

func FIFOLockKey(queue, groupID string) string {
	return fmt.Sprintf("kmsvc:fifo_lock:%s:%s", queue, groupID)
}

func QueueMetaKey(queue string) string {
	return fmt.Sprintf("kmsvc:queue:%s", queue)
}

func ShardMapKey(queue string) string {
	return fmt.Sprintf("kmsvc:shardmap:%s", queue)
}

func ShardMapChannel(queue string) string {
	return fmt.Sprintf("kmsvc:shardmap_invalidate:%s", queue)
}

func QueueMetaChannel(queue string) string {
	return fmt.Sprintf("kmsvc:queuemeta_invalidate:%s", queue)
}

func RedeliverKey(queue string) string {
	return fmt.Sprintf("kmsvc:redeliver:%s", queue)
}

func QueueLockKey(queue string) string {
	return fmt.Sprintf("kmsvc:queue_lock:%s", queue)
}
