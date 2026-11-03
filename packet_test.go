package litenetlib

import "testing"

// TestPacketPoolBounds follows NetPacketPool: at most PacketPoolSize packets are kept,
// big packets are never pooled, and reused packets lose their fragmented flag.
func TestPacketPoolBounds(t *testing.T) {
	var pool packetPool
	for range PacketPoolSize + 1 {
		pool.recycle(&packet{data: make([]byte, 10, maxPacketSize)})
	}
	if pool.count != PacketPoolSize {
		t.Fatalf("pool keeps %d packets, want %d", pool.count, PacketPoolSize)
	}
	pool.recycle(&packet{data: make([]byte, maxPacketSize+1)})
	if pool.count != PacketPoolSize {
		t.Fatal("full pool accepted another packet")
	}

	var empty packetPool
	empty.recycle(&packet{data: make([]byte, maxPacketSize+1)})
	if empty.count != 0 {
		t.Fatal("oversized packet was pooled")
	}
	p := &packet{data: make([]byte, 5, maxPacketSize)}
	p.markFragmented()
	empty.recycle(p)
	if q := empty.get(3, false); q != p || q.isFragmented() {
		t.Fatalf("reused packet %p fragmented=%v, want %p without the flag", q, q.isFragmented(), p)
	}
}
