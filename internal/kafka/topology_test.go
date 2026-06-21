package kafka

import "testing"

func TestShardTopicName(t *testing.T) {
	cases := []struct {
		name  string
		queue string
		fifo  bool
		shard string
		want  string
	}{
		{"standard", "orders", false, "0", "kmsvc.orders.shard-0"},
		{"fifo", "orders", true, "0", "kmsvc.orders.fifo.shard-0"},
		{"split child", "orders", true, "2", "kmsvc.orders.fifo.shard-2"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ShardTopicName(c.queue, c.fifo, c.shard); got != c.want {
				t.Errorf("ShardTopicName(%q, %v, %q) = %q, want %q", c.queue, c.fifo, c.shard, got, c.want)
			}
		})
	}
}

func TestDLQShardTopicName(t *testing.T) {
	if got, want := DLQShardTopicName("orders", false, "0"), "kmsvc.orders.dlq.shard-0"; got != want {
		t.Errorf("DLQShardTopicName = %q, want %q", got, want)
	}
	if got, want := DLQShardTopicName("orders", true, "0"), "kmsvc.orders.fifo.dlq.shard-0"; got != want {
		t.Errorf("DLQShardTopicName(fifo) = %q, want %q", got, want)
	}
}

func TestPartitionWithinShardDeterministic(t *testing.T) {
	const partitions = 6
	p1 := PartitionWithinShard("group-a", partitions)
	p2 := PartitionWithinShard("group-a", partitions)
	if p1 != p2 {
		t.Errorf("PartitionWithinShard not deterministic: %d != %d", p1, p2)
	}
	if p1 < 0 || p1 >= partitions {
		t.Errorf("PartitionWithinShard out of range: %d", p1)
	}
}

func TestPartitionWithinShardDistribution(t *testing.T) {
	const partitions = 6
	seen := map[int32]bool{}
	for i := 0; i < 1000; i++ {
		groupID := "group-" + string(rune('a'+i%26)) + string(rune('0'+i%10))
		p := PartitionWithinShard(groupID, partitions)
		seen[p] = true
	}
	if len(seen) < 2 {
		t.Errorf("expected groups to spread across multiple partitions, got %d distinct partitions", len(seen))
	}
}

func TestSelectShard(t *testing.T) {
	mid := SplitHashRange(0, FullHashRangeEnd)
	shards := []Shard{
		{ID: "1", Topic: "kmsvc.orders.fifo.shard-1", HashRangeStart: 0, HashRangeEnd: mid},
		{ID: "2", Topic: "kmsvc.orders.fifo.shard-2", HashRangeStart: mid, HashRangeEnd: FullHashRangeEnd},
	}
	for i := 0; i < 1000; i++ {
		key := "group-" + string(rune('a'+i%26)) + string(rune('0'+i%10))
		s, ok := SelectShard(shards, key)
		if !ok {
			t.Fatalf("no shard found for key %q", key)
		}
		h := HashKey(key)
		if h < s.HashRangeStart || h >= s.HashRangeEnd {
			t.Errorf("key %q hash %d outside selected shard range [%d, %d)", key, h, s.HashRangeStart, s.HashRangeEnd)
		}
	}
}

func TestSelectShardStableAcrossSplit(t *testing.T) {
	parent := []Shard{{ID: "0", Topic: "kmsvc.orders.fifo.shard-0", HashRangeStart: 0, HashRangeEnd: FullHashRangeEnd}}
	mid := SplitHashRange(0, FullHashRangeEnd)
	children := []Shard{
		{ID: "1", Topic: "kmsvc.orders.fifo.shard-1", HashRangeStart: 0, HashRangeEnd: mid},
		{ID: "2", Topic: "kmsvc.orders.fifo.shard-2", HashRangeStart: mid, HashRangeEnd: FullHashRangeEnd},
	}
	for i := 0; i < 1000; i++ {
		key := "group-" + string(rune('a'+i%26)) + string(rune('0'+i%10))
		before, _ := SelectShard(parent, key)
		after, ok := SelectShard(children, key)
		if !ok {
			t.Fatalf("no child shard found for key %q", key)
		}
		h := HashKey(key)
		wantParent := h < mid
		gotParentSide := after.ID == "1"
		if wantParent != gotParentSide {
			t.Errorf("key %q (hash %d) routed to wrong child after split", key, h)
		}
		_ = before
	}
}

func TestValidateNoDLQCycle(t *testing.T) {
	cases := []struct {
		name      string
		queue     string
		isDLQ     bool
		target    string
		expectErr bool
	}{
		{"no target", "orders", false, "", false},
		{"valid target", "orders", false, "orders-dlq", false},
		{"self reference", "orders", false, "orders", true},
		{"dlq with target", "orders-dlq", true, "another-dlq", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := ValidateNoDLQCycle(c.queue, c.isDLQ, c.target)
			if c.expectErr && err == nil {
				t.Errorf("expected error, got nil")
			}
			if !c.expectErr && err != nil {
				t.Errorf("expected no error, got %v", err)
			}
		})
	}
}
