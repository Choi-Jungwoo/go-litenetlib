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
	// macOS refuses 0.0.0.1; on Linux, `ip route add unreachable 203.0.113.0/24` makes
	// TEST-NET-3 fail with EHOSTUNREACH.
	var unreachable netip.AddrPort
	probe, _ := net.ListenUDP("udp4", nil)
	for _, candidate := range []string{"0.0.0.1:9", "203.0.113.1:9"} {
		addr := netip.MustParseAddrPort(candidate)
		if _, err := probe.WriteToUDPAddrPort([]byte{0}, addr); errors.Is(err, syscall.EHOSTUNREACH) {
			unreachable = addr
			break
		}
	}
	probe.Close()
	if !unreachable.IsValid() {
		t.Skip("no destination fails with EHOSTUNREACH here")
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
