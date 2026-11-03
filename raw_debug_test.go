//go:build debug

package litenetlib_test

import (
	"bytes"
	"testing"
	"time"

	litenetlib "github.com/choi-jungwoo/go-litenetlib"
)

// checkStatistics: debug builds count traffic (STATS_ENABLED) and can reset the counters.
func checkStatistics(t *testing.T, m *litenetlib.Manager, p *litenetlib.Peer, c *rawClient) {
	t.Helper()
	c.send(pPing, 5, 0)
	c.expect(pPong)
	deadline := time.Now().Add(time.Second)
	for m.Statistics().PacketsReceived < 2 || p.Statistics().PacketsSent == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("statistics never counted traffic: %+v %+v", m.Statistics(), p.Statistics())
		}
		p.Send([]byte{1}, litenetlib.Unreliable)
		p.Flush()
		time.Sleep(5 * time.Millisecond)
	}
	p.ResetStatistics()
	m.ResetStatistics()
	if s := p.Statistics(); s != (litenetlib.Statistics{}) {
		t.Errorf("peer statistics after reset: %+v", s)
	}
	if s := m.Statistics(); s.PacketsReceived != 0 || s.BytesSent != 0 {
		t.Errorf("manager statistics after reset: %+v", s)
	}
}

func TestRawSimulatePacketLoss(t *testing.T) {
	m := startServer(t, &litenetlib.EventBasedListener{}, func(m *litenetlib.Manager) {
		m.SimulatePacketLoss = true
		m.SimulationPacketLossChance = 100
	})
	c := newRawClient(t, m.LocalPort())
	c.send(pUnreliable)
	c.expectNothing() // the datagram was dropped before processing
}

func TestRawSimulateLatency(t *testing.T) {
	m := startServer(t, &litenetlib.EventBasedListener{}, func(m *litenetlib.Manager) {
		m.SimulateLatency = true
		m.SimulationMinLatency = 200
		m.SimulationMaxLatency = 200
	})
	c := newRawClient(t, m.LocalPort())
	start := time.Now()
	c.send(pUnreliable)
	if d := c.recv(time.Second); !bytes.Equal(d, []byte{pPeerNotFound}) {
		t.Fatalf("got % x, want delayed PeerNotFound", d)
	}
	if held := time.Since(start); held < 200*time.Millisecond || held > 400*time.Millisecond {
		t.Fatalf("incoming datagram held %v, want about 200ms", held)
	}
}
