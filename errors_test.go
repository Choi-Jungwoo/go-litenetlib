package litenetlib

import (
	"errors"
	"net"
	"net/netip"
	"syscall"
	"testing"
	"time"
)

// TestHostUnreachableDisconnectsPeer follows NetManager.SendRaw: a HostUnreachable send
// error force-disconnects the peer, then raises a network error event.
func TestHostUnreachableDisconnectsPeer(t *testing.T) {
	unreachable := netip.MustParseAddrPort("0.0.0.1:9")
	probe, _ := net.ListenUDP("udp4", nil)
	_, err := probe.WriteToUDPAddrPort([]byte{0}, unreachable)
	probe.Close()
	if !errors.Is(err, syscall.EHOSTUNREACH) {
		t.Skipf("platform does not report EHOSTUNREACH for %v: %v", unreachable, err)
	}

	var events []string
	m := NewManager(&EventBasedListener{
		PeerDisconnected: func(_ *Peer, info DisconnectInfo) {
			if info.Reason == HostUnreachable && info.SocketError != nil {
				events = append(events, "disconnect")
			}
		},
		NetworkError: func(addr netip.AddrPort, err error) {
			if addr == unreachable && err != nil {
				events = append(events, "error")
			}
		},
	})
	if err := m.Start(0); err != nil {
		t.Fatal(err)
	}
	defer m.Stop(false)
	p := m.tryAddPeer(newPeer(m, unreachable, 0))
	p.accept(1, 0) // the accept packet send fails

	deadline := time.Now().Add(time.Second)
	for len(events) < 2 && time.Now().Before(deadline) {
		m.PollEvents()
		time.Sleep(5 * time.Millisecond)
	}
	if len(events) != 2 || events[0] != "disconnect" || events[1] != "error" {
		t.Fatalf("events %v, want [disconnect error]", events)
	}
	if p.ConnectionState() != StateDisconnected {
		t.Fatalf("state %v, want Disconnected", p.ConnectionState())
	}
}
