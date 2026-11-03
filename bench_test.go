package litenetlib

import (
	"encoding/binary"
	"net/netip"
	"testing"
)

// benchPeer returns a manager (not started, so sends stop before the socket) with one connected peer.
func benchPeer(b *testing.B) (*Manager, *Peer) {
	m := NewManager(&EventBasedListener{})
	m.AutoRecycle = true
	p := m.tryAddPeer(newPeer(m, netip.MustParseAddrPort("127.0.0.1:9000"), 0))
	p.accept(1, 0)
	return m, p
}

func BenchmarkReceiveReliableOrdered(b *testing.B) {
	m, p := benchPeer(b)
	datagram := make([]byte, SequencedHeaderSize+64)
	datagram[0] = byte(propReliableOrdered)
	b.ReportAllocs()
	for i := range b.N {
		binary.LittleEndian.PutUint16(datagram[1:], uint16(i%MaxSequence))
		m.dataReceived(datagram, p.addr)
		m.PollEvents()
	}
}

func BenchmarkReceiveMerged(b *testing.B) {
	m, p := benchPeer(b)
	// 10 unreliable packets of 32 bytes in one merged datagram.
	datagram := []byte{byte(propMerged)}
	for range 10 {
		datagram = binary.LittleEndian.AppendUint16(datagram, 33)
		datagram = append(datagram, make([]byte, 33)...)
	}
	b.ReportAllocs()
	for range b.N {
		m.dataReceived(datagram, p.addr)
		m.PollEvents()
	}
}

func benchSendFlush(b *testing.B, method DeliveryMethod) {
	_, p := benchPeer(b)
	data := make([]byte, 64)
	b.ReportAllocs()
	for range b.N {
		if err := p.Send(data, method); err != nil {
			b.Fatal(err)
		}
		p.Flush()
	}
}

func BenchmarkSendFlushUnreliable(b *testing.B) { benchSendFlush(b, Unreliable) }
func BenchmarkSendFlushSequenced(b *testing.B)  { benchSendFlush(b, Sequenced) }

// BenchmarkSendFlushAckReliableOrdered sends one reliable packet and processes its ack.
func BenchmarkSendFlushAckReliableOrdered(b *testing.B) {
	m, p := benchPeer(b)
	data := make([]byte, 64)
	ack := make([]byte, SequencedHeaderSize+DefaultWindowSize/8)
	ack[0] = byte(propAckReliableOrdered)
	b.ReportAllocs()
	for i := range b.N {
		if err := p.Send(data, ReliableOrdered); err != nil {
			b.Fatal(err)
		}
		p.Flush()
		seq := i % MaxSequence
		clear(ack[SequencedHeaderSize:])
		binary.LittleEndian.PutUint16(ack[1:], uint16(seq))
		ack[SequencedHeaderSize+(seq%DefaultWindowSize)/8] = 1 << (seq % 8)
		m.dataReceived(ack, p.addr)
	}
}

func BenchmarkSendFragmented(b *testing.B) {
	_, p := benchPeer(b)
	data := make([]byte, 16*1024)
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	for range b.N {
		if err := p.Send(data, ReliableUnordered); err != nil {
			b.Fatal(err)
		}
		p.queueMu.Lock()
		for _, pk := range p.reliableUnordered.queue {
			recyclePacket(pk)
		}
		p.reliableUnordered.queue = p.reliableUnordered.queue[:0]
		p.queueMu.Unlock()
	}
}

func BenchmarkDataWriterReader(b *testing.B) {
	w := NewDataWriter()
	r := &DataReader{}
	b.ReportAllocs()
	for range b.N {
		w.Reset()
		w.PutInt(42)
		w.PutFloat(1.5)
		w.PutString("player")
		w.PutULong(7)
		r.SetSource(w.Data())
		_ = r.GetInt()
		_ = r.GetFloat()
		_ = r.GetString()
		_ = r.GetULong()
	}
}

// BenchmarkLoopbackSendUnreliable includes the real socket write.
func BenchmarkLoopbackSendUnreliable(b *testing.B) {
	m := NewManager(&EventBasedListener{})
	if err := m.Start(0); err != nil {
		b.Fatal(err)
	}
	defer m.Stop(false)
	sink, err := listenUDP("udp4", netip.MustParseAddrPort("127.0.0.1:0"), false)
	if err != nil {
		b.Fatal(err)
	}
	defer sink.Close()
	p := m.tryAddPeer(newPeer(m, netip.MustParseAddrPort(sink.LocalAddr().String()), 0))
	p.accept(1, 0)
	data := make([]byte, 64)
	b.ReportAllocs()
	for range b.N {
		_ = p.Send(data, Unreliable)
		p.Flush()
	}
}
