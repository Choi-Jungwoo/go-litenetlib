package litenetlib_test

import (
	"math/rand/v2"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"
)

// startLossyProxy relays UDP between one client and the server on serverPort,
// dropping 15%, duplicating 5% and delaying (thus reordering) packets by 0-40ms
// in both directions. It returns the port clients should use.
func startLossyProxy(t *testing.T, serverPort int) int {
	t.Helper()
	front, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	back, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	server := netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(serverPort))
	var mu sync.Mutex
	var client netip.AddrPort
	rng := rand.New(rand.NewPCG(1, 2))

	relay := func(to *net.UDPConn, data []byte, dst netip.AddrPort) {
		mu.Lock()
		drop, dup := rng.Float64() < 0.15, rng.Float64() < 0.05
		delay := time.Duration(rng.IntN(40)) * time.Millisecond
		mu.Unlock()
		if drop {
			return
		}
		b := append([]byte(nil), data...)
		send := func() { _, _ = to.WriteToUDPAddrPort(b, dst) }
		time.AfterFunc(delay, send)
		if dup {
			time.AfterFunc(delay+5*time.Millisecond, send)
		}
	}
	pump := func(from, to *net.UDPConn, forward bool) {
		buf := make([]byte, 2048)
		for {
			n, addr, err := from.ReadFromUDPAddrPort(buf)
			if err != nil {
				return
			}
			mu.Lock()
			if forward {
				client = addr
			}
			dst := client
			mu.Unlock()
			if forward {
				dst = server
			}
			relay(to, buf[:n], dst)
		}
	}
	go pump(front, back, true)
	go pump(back, front, false)
	t.Cleanup(func() { front.Close(); back.Close() })
	return front.LocalAddr().(*net.UDPAddr).Port
}
