package litenetlib

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"sync"
)

// LocalAddrType selects the address families returned by GetLocalIPList.
type LocalAddrType byte

const (
	LocalAddrIPv4 LocalAddrType = 1
	LocalAddrIPv6 LocalAddrType = 2
	LocalAddrAll                = LocalAddrIPv4 | LocalAddrIPv6
)

// ipv6Support mirrors NetSocket.IPv6Support (Socket.OSSupportsIPv6).
var ipv6Support = sync.OnceValue(func() bool {
	c, err := net.ListenPacket("udp6", "[::1]:0")
	if err != nil {
		return false
	}
	c.Close()
	return true
})

// ResolveAddress parses or resolves host like NetUtils.ResolveAddress: IPv6 is preferred
// when supported and "localhost" maps to the IPv6 loopback.
func ResolveAddress(host string) (netip.Addr, error) {
	if addr, err := netip.ParseAddr(host); err == nil {
		return addr, nil
	}
	if ipv6Support() {
		if host == "localhost" {
			return netip.IPv6Loopback(), nil
		}
		if addr, ok := resolveFamily(host, "ip6"); ok {
			return addr, nil
		}
	}
	if addr, ok := resolveFamily(host, "ip4"); ok {
		return addr, nil
	}
	return netip.Addr{}, fmt.Errorf("litenetlib: invalid address: %s", host)
}

func resolveFamily(host, network string) (netip.Addr, bool) {
	addrs, err := net.DefaultResolver.LookupNetIP(context.Background(), network, host)
	if err != nil || len(addrs) == 0 {
		return netip.Addr{}, false
	}
	return addrs[0].Unmap(), true
}

// MakeEndPoint resolves host and pairs it with port.
func MakeEndPoint(host string, port int) (netip.AddrPort, error) {
	if err := checkPort(port); err != nil {
		return netip.AddrPort{}, err
	}
	addr, err := ResolveAddress(host)
	return netip.AddrPortFrom(addr, uint16(port)), err
}

// checkPort rejects ports outside 0..65535, as the reference's IPEndPoint constructor does.
func checkPort(port int) error {
	if port < 0 || port > 0xFFFF {
		return fmt.Errorf("litenetlib: port %d out of range", port)
	}
	return nil
}

// GetLocalIPList returns the local addresses of the requested families (NetUtils.GetLocalIpList):
// addresses of up, non-loopback interfaces with a gateway, else the addresses of the host
// name, else the loopback addresses.
func GetLocalIPList(addrType LocalAddrType) []string {
	wantV4, wantV6 := addrType&LocalAddrIPv4 != 0, addrType&LocalAddrIPv6 != 0
	wanted := func(a netip.Addr) bool { return (wantV4 && a.Is4()) || (wantV6 && a.Is6()) }
	var list []string

	hasGateway := gatewayInterfaces()
	ifaces, _ := net.Interfaces()
	for _, ifi := range ifaces {
		if ifi.Flags&net.FlagLoopback != 0 || ifi.Flags&net.FlagUp == 0 || !hasGateway(ifi) {
			continue
		}
		ifAddrs, _ := ifi.Addrs()
		for _, a := range ifAddrs {
			ipn, ok := a.(*net.IPNet)
			if !ok {
				continue
			}
			addr, _ := netip.AddrFromSlice(ipn.IP)
			if addr = addr.Unmap(); wanted(addr) {
				list = append(list, addr.String())
			}
		}
	}

	if len(list) == 0 { // fallback: addresses of the host name
		if name, err := os.Hostname(); err == nil {
			addrs, _ := net.DefaultResolver.LookupNetIP(context.Background(), "ip", name)
			for _, a := range addrs {
				if a = a.Unmap(); wanted(a) {
					list = append(list, a.String())
				}
			}
		}
	}
	if len(list) == 0 {
		if wantV4 {
			list = append(list, "127.0.0.1")
		}
		if wantV6 {
			list = append(list, "::1")
		}
	}
	return list
}

// GetLocalIP returns the first local address of the requested families, or "".
func GetLocalIP(addrType LocalAddrType) string {
	if list := GetLocalIPList(addrType); len(list) > 0 {
		return list[0]
	}
	return ""
}

// StartHost listens on the given IPv4 and IPv6 addresses, resolving host names.
func (m *Manager) StartHost(addr4, addr6 string, port int) error {
	a4, err4 := ResolveAddress(addr4)
	a6, err6 := ResolveAddress(addr6)
	if err := errors.Join(err4, err6); err != nil {
		return err
	}
	return m.StartAddr(a4, a6, port)
}
