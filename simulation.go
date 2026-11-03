package litenetlib

import (
	"math/rand/v2"
	"net/netip"
	"time"
)

const minLatencyThreshold = 5

type delayedDatagram struct {
	data []byte
	addr netip.AddrPort
	at   time.Time
}

// simulate applies the packet loss and latency simulation to an incoming datagram.
// It reports whether the datagram was consumed (dropped or held).
func (m *Manager) simulate(data []byte, addr netip.AddrPort) bool {
	if m.SimulatePacketLoss && rand.Float64()*100 < float64(m.SimulationPacketLossChance) {
		return true
	}
	if !m.SimulateLatency {
		return false
	}
	latency := m.SimulationMinLatency // Random.Next(min, max) returns min when max <= min
	if m.SimulationMaxLatency > m.SimulationMinLatency {
		latency += rand.IntN(m.SimulationMaxLatency - m.SimulationMinLatency)
	}
	if latency <= minLatencyThreshold {
		return false
	}
	held := delayedDatagram{append([]byte(nil), data...), addr, time.Now().Add(time.Duration(latency) * time.Millisecond)}
	m.simMu.Lock()
	m.delayed = append(m.delayed, held)
	m.simMu.Unlock()
	return true
}

// releaseDelayed processes the held datagrams whose time has come (logic goroutine).
func (m *Manager) releaseDelayed() {
	now := time.Now()
	var due []delayedDatagram
	m.simMu.Lock()
	kept := m.delayed[:0]
	for _, d := range m.delayed {
		if d.at.After(now) {
			kept = append(kept, d)
		} else {
			due = append(due, d)
		}
	}
	clear(m.delayed[len(kept):])
	m.delayed = kept
	m.simMu.Unlock()
	for _, d := range due {
		m.dataReceived(d.data, d.addr)
	}
}
