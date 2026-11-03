package litenetlib_test

import (
	"context"
	"errors"
	"maps"
	"net"
	"net/netip"
	"os/exec"
	"runtime"
	"slices"
	"strings"
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

// TestResolveAddressFailure uses a resolver whose DNS server is unreachable, so the
// lookup fails regardless of the machine's DNS setup.
func TestResolveAddressFailure(t *testing.T) {
	saved := net.DefaultResolver
	net.DefaultResolver = &net.Resolver{
		PreferGo: true,
		Dial: func(context.Context, string, string) (net.Conn, error) {
			return nil, errors.New("no DNS server")
		},
	}
	defer func() { net.DefaultResolver = saved }()

	if a, err := litenetlib.ResolveAddress("example.com"); err == nil {
		t.Fatalf("resolved %v without a DNS server", a)
	}
	if _, err := litenetlib.MakeEndPoint("example.com", 1); err == nil {
		t.Fatal("MakeEndPoint resolved without a DNS server")
	}
	if a, err := litenetlib.ResolveAddress("::1"); err != nil || a != netip.IPv6Loopback() {
		t.Fatalf("literal addresses must not need DNS: %v %v", a, err)
	}
}

// gatewayInterfacesFromTools lists interfaces with a gateway route using the system
// tools, an independent view of the routing table.
func gatewayInterfacesFromTools(t *testing.T) map[string]bool {
	names := map[string]bool{}
	switch runtime.GOOS {
	case "darwin":
		out, err := exec.Command("netstat", "-rn", "-f", "inet").Output()
		if err != nil {
			t.Skip("netstat unavailable:", err)
		}
		for _, line := range strings.Split(string(out), "\n") {
			// Destination Gateway Flags Netif [Expire]
			if f := strings.Fields(line); len(f) >= 4 && strings.Contains(f[2], "G") {
				names[f[3]] = true
			}
		}
	case "linux":
		for _, family := range []string{"-4", "-6"} {
			out, err := exec.Command("ip", family, "route", "show", "table", "all").Output()
			if err != nil {
				t.Skip("ip unavailable:", err)
			}
			for _, line := range strings.Split(string(out), "\n") {
				f := strings.Fields(line)
				if i := slices.Index(f, "dev"); i >= 0 && i+1 < len(f) && slices.Contains(f, "via") {
					names[f[i+1]] = true
				}
			}
		}
	default:
		t.Skip("no routing tool check for", runtime.GOOS)
	}
	return names
}

func TestGetLocalIPListUsesGatewayInterfaces(t *testing.T) {
	want := map[string]bool{}
	ifaces, _ := net.Interfaces()
	gateways := gatewayInterfacesFromTools(t)
	for _, ifi := range ifaces {
		if !gateways[ifi.Name] || ifi.Flags&net.FlagUp == 0 || ifi.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, _ := ifi.Addrs()
		for _, a := range addrs {
			if ipn, ok := a.(*net.IPNet); ok && ipn.IP.To4() != nil {
				want[ipn.IP.String()] = true
			}
		}
	}
	if len(want) == 0 {
		t.Skip("no IPv4 interface with a gateway; the fallback path applies")
	}
	got := map[string]bool{}
	for _, s := range litenetlib.GetLocalIPList(litenetlib.LocalAddrIPv4) {
		got[s] = true
	}
	if !maps.Equal(got, want) {
		t.Fatalf("GetLocalIPList(IPv4) = %v, want addresses of gateway interfaces %v (gateways: %v)", got, want, gateways)
	}
}
