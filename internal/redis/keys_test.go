package redis

import "testing"

func TestKeyFormats(t *testing.T) {
	cases := []struct {
		name string
		got  string
		want string
	}{
		{"inflight", InFlightKey("orders", "rh-1"), "kmsvc:inflight:orders:rh-1"},
		{"pending", PendingKey("orders", "0", 3), "kmsvc:pending:orders:0:3"},
		{"watermark", WatermarkKey("orders", "0", 3), "kmsvc:watermark:orders:0:3"},
		{"vis_index", VisIndexKey("orders"), "kmsvc:vis_index:orders"},
		{"dedup", DedupKey("orders", "g1", "d1"), "kmsvc:dedup:orders:g1:d1"},
		{"fifo_lock", FIFOLockKey("orders", "g1"), "kmsvc:fifo_lock:orders:g1"},
		{"queue", QueueMetaKey("orders"), "kmsvc:queue:orders"},
		{"shardmap", ShardMapKey("orders"), "kmsvc:shardmap:orders"},
		{"redeliver", RedeliverKey("orders"), "kmsvc:redeliver:orders"},
		{"queue_lock", QueueLockKey("orders"), "kmsvc:queue_lock:orders"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.got != c.want {
				t.Errorf("got %q, want %q", c.got, c.want)
			}
		})
	}
}
