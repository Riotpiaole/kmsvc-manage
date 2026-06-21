// Package kafka implements the queue-name <-> Kafka-topic mapping and
// topic admin operations described in design.md §6.
package kafka

import (
	"fmt"
)

const (
	topicPrefix           = "kmsvc."
	fifoSuffix            = ".fifo"
	dlqSuffix             = ".dlq"
	shardInfix            = ".shard-"
	DefaultPartitionCount = 6

	// FullHashRangeEnd is the exclusive upper bound of the 32-bit shard key
	// space; a queue's first shard owns [0, FullHashRangeEnd).
	FullHashRangeEnd uint32 = 0xFFFFFFFF
)

// ShardTopicName returns the Kafka topic name for one shard of a queue, per
// design.md §6: kmsvc.{queueName}.shard-{id} (standard),
// kmsvc.{queueName}.fifo.shard-{id} (FIFO).
func ShardTopicName(queueName string, fifo bool, shardID string) string {
	base := topicPrefix + queueName
	if fifo {
		base += fifoSuffix
	}
	return base + shardInfix + shardID
}

// DLQShardTopicName returns the DLQ topic name for one shard of a queue's
// DLQ, per design.md §6: kmsvc.{queueName}.dlq.shard-{id} / .fifo.dlq.shard-{id}.
func DLQShardTopicName(queueName string, fifo bool, shardID string) string {
	base := topicPrefix + queueName
	if fifo {
		base += fifoSuffix
	}
	return base + dlqSuffix + shardInfix + shardID
}

// ConsumerGroup returns the single Kafka consumer-group name shared by every
// message-plane replica consuming a queue's shards, used both for normal
// consumption and by the queue-operator's drain check (design.md §2c, §9).
func ConsumerGroup(queueName string) string {
	return "kmsvc-consumer-" + queueName
}

// murmur2 mirrors Kafka's default partitioner hash (murmur2), used so that
// FIFO partition assignment here matches what a native Kafka producer would
// compute for the same key, per design.md §6.
func murmur2(data []byte) uint32 {
	const (
		seed uint32 = 0x9747b28c
		m    uint32 = 0x5bd1e995
		r           = 24
	)
	length := len(data)
	h := seed ^ uint32(length)
	four := length / 4

	for i := 0; i < four; i++ {
		i4 := i * 4
		k := uint32(data[i4]&0xff) |
			(uint32(data[i4+1]&0xff) << 8) |
			(uint32(data[i4+2]&0xff) << 16) |
			(uint32(data[i4+3]&0xff) << 24)
		k *= m
		k ^= k >> r
		k *= m
		h *= m
		h ^= k
	}

	switch length & 3 {
	case 3:
		h ^= uint32(data[(length&^3)+2]&0xff) << 16
		fallthrough
	case 2:
		h ^= uint32(data[(length&^3)+1]&0xff) << 8
		fallthrough
	case 1:
		h ^= uint32(data[length&^3] & 0xff)
		h *= m
	}

	h ^= h >> 13
	h *= m
	h ^= h >> 15

	return h
}

// toPositive mirrors Kafka's Utils.toPositive, masking the sign bit so the
// result is usable as an unsigned partition index.
func toPositive(n uint32) uint32 {
	return n & 0x7fffffff
}

// HashKey returns the murmur2 hash of a routing key (MessageGroupId for FIFO
// queues, a random UUID for standard queues) into the shard key space used
// for both shard selection and within-shard partitioning (design.md §2c, §6).
func HashKey(key string) uint32 {
	return toPositive(murmur2([]byte(key)))
}

// Shard is the subset of a Queue's status.shards entry needed for routing.
// Phase mirrors apis/kmsvc/v1.ShardPhase as a plain string to avoid this
// package depending on the CRD API package.
type Shard struct {
	ID             string
	Topic          string
	HashRangeStart uint32
	HashRangeEnd   uint32
	Phase          string
}

// ActiveShards filters to shards eligible to receive newly-sent messages
// (design.md §2c: a `Closing` shard keeps being consumed/drained but stops
// being a write target).
func ActiveShards(shards []Shard) []Shard {
	active := make([]Shard, 0, len(shards))
	for _, s := range shards {
		if s.Phase == "" || s.Phase == "Active" {
			active = append(active, s)
		}
	}
	return active
}

// SelectShard returns the shard whose hash range contains routingKey's hash,
// per design.md §2c. Callers doing write-path routing should pass
// ActiveShards(shards) so messages never target a `Closing` shard; the
// shards passed in must cover the full key space with no gaps for this to
// always find a match.
func SelectShard(shards []Shard, routingKey string) (Shard, bool) {
	h := HashKey(routingKey)
	for _, s := range shards {
		if h >= s.HashRangeStart && h < s.HashRangeEnd {
			return s, true
		}
	}
	return Shard{}, false
}

// SplitHashRange returns the midpoint of [start, end), the boundary between
// the two child shards created by a split (design.md §2c).
func SplitHashRange(start, end uint32) uint32 {
	return start + (end-start)/2
}

// PartitionWithinShard returns the partition a message lands on within its
// shard's topic. For FIFO queues routingKey is the MessageGroupId, ensuring
// all messages for a group are ordered on one partition within that shard
// (design.md §3, §6); for standard queues routingKey is a random per-message
// value, so traffic just spreads evenly.
func PartitionWithinShard(routingKey string, partitionsPerShard int32) int32 {
	if partitionsPerShard <= 0 {
		partitionsPerShard = DefaultPartitionCount
	}
	return int32(HashKey(routingKey) % uint32(partitionsPerShard))
}

// ValidateNoDLQCycle enforces design.md §5's DLQ-loop guard: a queue's
// dead-letter target must not be itself, and a queue marked as a DLQ must
// not itself have a dead-letter target (no DLQ chains).
func ValidateNoDLQCycle(queueName string, isDLQ bool, deadLetterTarget string) error {
	if deadLetterTarget == "" {
		return nil
	}
	if deadLetterTarget == queueName {
		return fmt.Errorf("queue %q cannot set its own dead-letter target", queueName)
	}
	if isDLQ {
		return fmt.Errorf("DLQ queue %q cannot itself have a dead-letter target (no DLQ chains)", queueName)
	}
	return nil
}
