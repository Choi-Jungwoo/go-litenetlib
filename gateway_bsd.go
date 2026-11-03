//go:build darwin || dragonfly || freebsd || netbsd || openbsd

package litenetlib

import (
	"net"
	"syscall"

	"golang.org/x/net/route"
)

// gatewayInterfaces reports which interfaces have an IPv4 gateway route, issuing the
// sysctl of .NET's EnumerateGatewayAddressesForInterface:
// {CTL_NET, PF_ROUTE, 0, AF_INET, NET_RT_FLAGS, RTF_UP|RTF_GATEWAY}.
func gatewayInterfaces() func(net.Interface) bool {
	indexes := map[int]bool{}
	rib, err := route.FetchRIB(syscall.AF_INET, syscall.NET_RT_FLAGS, syscall.RTF_UP|syscall.RTF_GATEWAY)
	if err == nil {
		msgs, _ := route.ParseRIB(route.RIBTypeRoute, rib)
		for _, msg := range msgs {
			rm, ok := msg.(*route.RouteMessage)
			if !ok || rm.Flags&syscall.RTF_GATEWAY == 0 || len(rm.Addrs) <= syscall.RTAX_GATEWAY {
				continue
			}
			if _, ok := rm.Addrs[syscall.RTAX_GATEWAY].(*route.Inet4Addr); ok {
				indexes[rm.Index] = true
			}
		}
	}
	return func(ifi net.Interface) bool { return indexes[ifi.Index] }
}
