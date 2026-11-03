package litenetlib_test

import (
	"net"
	"net/netip"
	"testing"

	litenetlib "github.com/choi-jungwoo/go-litenetlib"
)

func ipv6Supported() bool {
	c, err := net.ListenPacket("udp6", "[::1]:0")
	if err == nil {
		c.Close()
	}
	return err == nil
}

func TestResolveAddress(t *testing.T) {
	if a, err := litenetlib.ResolveAddress("10.1.2.3"); err != nil || a != netip.MustParseAddr("10.1.2.3") {
		t.Errorf("literal: %v %v", a, err)
	}
	// NetUtils.ResolveAddress prefers IPv6 and maps "localhost" to the IPv6 loopback.
	want := netip.MustParseAddr("127.0.0.1")
	if ipv6Supported() {
		want = netip.IPv6Loopback()
	}
	if a, err := litenetlib.ResolveAddress("localhost"); err != nil || a != want {
		t.Errorf("localhost: %v %v, want %v", a, err, want)
	}
	if ep, err := litenetlib.MakeEndPoint("10.1.2.3", 77); err != nil || ep.String() != "10.1.2.3:77" {
		t.Errorf("MakeEndPoint: %v %v", ep, err)
	}
}

func TestGetLocalIP(t *testing.T) {
	v4 := litenetlib.GetLocalIPList(litenetlib.LocalAddrIPv4)
	if len(v4) == 0 {
		t.Fatal("no IPv4 address, not even the loopback fallback")
	}
	for _, s := range v4 {
		if a, err := netip.ParseAddr(s); err != nil || !a.Is4() {
			t.Errorf("IPv4 list contains %q", s)
		}
	}
	if s := litenetlib.GetLocalIP(litenetlib.LocalAddrIPv6); !netip.MustParseAddr(s).Is6() {
		t.Errorf("GetLocalIP(IPv6) = %q", s)
	}
}
