//go:build !debug

package litenetlib_test

import (
	"bytes"
	"testing"
	"time"

	litenetlib "github.com/choi-jungwoo/go-litenetlib"
)

// checkStatistics: release builds do not count traffic, as STATS_ENABLED is off.
func checkStatistics(t *testing.T, m *litenetlib.Manager, p *litenetlib.Peer, c *rawClient) {
	t.Helper()
	c.send(pPing, 5, 0)
	c.expect(pPong)
	p.Send([]byte{1}, litenetlib.Unreliable)
	p.Flush()
	time.Sleep(50 * time.Millisecond)
	if s := m.Statistics(); s != (litenetlib.Statistics{}) {
		t.Errorf("release manager statistics %+v, want zero", s)
	}
	if s := p.Statistics(); s.PacketsSent != 0 || s.BytesSent != 0 {
		t.Errorf("release peer statistics %+v, want zero", s)
	}
}

// Release builds ignore the simulation settings, like the reference without DEBUG.
func TestRawSimulationInactiveInRelease(t *testing.T) {
	m := startServer(t, &litenetlib.EventBasedListener{}, func(m *litenetlib.Manager) {
		m.SimulatePacketLoss = true
		m.SimulationPacketLossChance = 100
	})
	c := newRawClient(t, m.LocalPort())
	c.send(pUnreliable)
	if d := c.recv(time.Second); !bytes.Equal(d, []byte{pPeerNotFound}) {
		t.Fatalf("got % x, want an undropped PeerNotFound reply", d)
	}
}
