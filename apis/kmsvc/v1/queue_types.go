package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// QueuePhase is the reconciliation phase of a Queue, per design.md §2a.
type QueuePhase string

const (
	QueuePhasePending QueuePhase = "Pending"
	QueuePhaseReady   QueuePhase = "Ready"
	QueuePhaseFailed  QueuePhase = "Failed"
)

// QueueSpec defines the desired state of a Queue (design.md §2a).
type QueueSpec struct {
	// FIFOQueue enables per-MessageGroupId ordering and deduplication semantics.
	// +optional
	// +kubebuilder:default=false
	FIFOQueue bool `json:"fifoQueue,omitempty"`

	// VisibilityTimeoutSeconds is how long a received-but-unacked message stays
	// invisible to other consumers before being redelivered.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=43200
	// +kubebuilder:default=30
	VisibilityTimeoutSeconds int32 `json:"visibilityTimeoutSeconds,omitempty"`

	// MessageRetentionPeriodSeconds maps to the underlying Kafka topic's retention.ms.
	// +kubebuilder:validation:Minimum=60
	// +kubebuilder:validation:Maximum=1209600
	// +kubebuilder:default=345600
	MessageRetentionPeriodSeconds int32 `json:"messageRetentionPeriodSeconds,omitempty"`

	// MaxReceiveCount is how many times a message may be redelivered before
	// being routed to DeadLetterTargetQueue.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=5
	MaxReceiveCount int32 `json:"maxReceiveCount,omitempty"`

	// DeadLetterTargetQueue is the name of another Queue to route exhausted
	// messages to. Must not point at itself or at another DLQ (design.md §5).
	// +optional
	DeadLetterTargetQueue string `json:"deadLetterTargetQueue,omitempty"`

	// PartitionsPerShard is the Kafka partition count on each shard's topic.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=6
	PartitionsPerShard int32 `json:"partitionsPerShard,omitempty"`

	// DelaySeconds is the default delivery delay applied to sent messages.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=900
	// +optional
	DelaySeconds int32 `json:"delaySeconds,omitempty"`

	// IsDLQ marks this queue as itself a dead-letter queue, used to enforce
	// the no-DLQ-chaining validation rule in design.md §5.
	// +optional
	IsDLQ bool `json:"isDLQ,omitempty"`

	// MinShards is the floor on shard count; the operator never merges below this.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1
	MinShards int32 `json:"minShards,omitempty"`

	// MaxShards is the ceiling on shard count the operator may split up to (design.md §2c).
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=8
	MaxShards int32 `json:"maxShards,omitempty"`

	// ShardSplitThresholdBytesPerSec is the sustained per-shard throughput that
	// triggers a split into two child shards (design.md §2c).
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=5242880
	ShardSplitThresholdBytesPerSec int64 `json:"shardSplitThresholdBytesPerSec,omitempty"`

	// ShardSplitCooldownSeconds is the minimum age a shard must reach before it
	// is eligible to be split again, preventing rapid re-splitting of a child
	// that hasn't yet absorbed its share of traffic.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=300
	ShardSplitCooldownSeconds int32 `json:"shardSplitCooldownSeconds,omitempty"`
}

// ShardPhase is the lifecycle phase of an individual shard.
type ShardPhase string

const (
	ShardPhaseActive  ShardPhase = "Active"
	ShardPhaseClosing ShardPhase = "Closing"
	ShardPhaseClosed  ShardPhase = "Closed"
)

// ShardStatus describes one shard backing a Queue (design.md §2a/§2c).
type ShardStatus struct {
	// ID is the shard's identifier, used in its topic name (kmsvc.{queue}.shard-{id}).
	ID string `json:"id"`

	// Topic is the underlying Kafka topic name for this shard.
	Topic string `json:"topic"`

	// HashRangeStart/HashRangeEnd define the [start, end) murmur2 hash range
	// this shard owns over the 32-bit key space.
	HashRangeStart uint32 `json:"hashRangeStart"`
	HashRangeEnd   uint32 `json:"hashRangeEnd"`

	// Phase is this shard's lifecycle state.
	// +kubebuilder:validation:Enum=Active;Closing;Closed
	Phase ShardPhase `json:"phase"`

	// ParentID is the shard ID this shard was split from, empty for the
	// original shard-0.
	// +optional
	ParentID string `json:"parentId,omitempty"`

	// CreatedAt timestamps when this shard was created, used to enforce
	// ShardSplitCooldownSeconds.
	CreatedAt metav1.Time `json:"createdAt,omitempty"`
}

// QueueStatus defines the observed state of a Queue.
type QueueStatus struct {
	// Phase is the current reconciliation phase.
	// +kubebuilder:validation:Enum=Pending;Ready;Failed
	Phase QueuePhase `json:"phase,omitempty"`

	// Shards lists every shard backing this queue, active or draining
	// (design.md §2a/§2c).
	// +optional
	Shards []ShardStatus `json:"shards,omitempty"`

	// Conditions hold detailed status information.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="FIFO",type=boolean,JSONPath=`.spec.fifoQueue`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:resource:shortName=queue;queues

// Queue is the Schema for the queues API — see design.md §2a.
type Queue struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   QueueSpec   `json:"spec,omitempty"`
	Status QueueStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// QueueList contains a list of Queue.
type QueueList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Queue `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Queue{}, &QueueList{})
}
