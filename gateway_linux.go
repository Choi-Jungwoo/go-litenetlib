package litenetlib

import (
	"net"
	"os"
	"strconv"
	"strings"
)

// gatewayInterfaces reports which interfaces have a gateway, reading the route files
// like .NET's LinuxIPInterfaceProperties.GatewayAddresses.
func gatewayInterfaces() func(net.Interface) bool {
	names := map[string]bool{}
	// /proc/net/route: Iface Destination Gateway ... (hex IPv4)
	for _, f := range routeFileFields("/proc/net/route") {
		if len(f) > 2 {
			if gw, err := strconv.ParseUint(f[2], 16, 32); err == nil && gw != 0 {
				names[f[0]] = true
			}
		}
	}
	// /proc/net/ipv6_route: dest destLen src srcLen nextHop metric refcnt use flags iface
	for _, f := range routeFileFields("/proc/net/ipv6_route") {
		if len(f) > 9 && strings.Trim(f[4], "0") != "" {
			names[f[9]] = true
		}
	}
	return func(ifi net.Interface) bool { return names[ifi.Name] }
}

func routeFileFields(path string) [][]string {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var rows [][]string
	for _, line := range strings.Split(string(b), "\n") {
		rows = append(rows, strings.Fields(line))
	}
	return rows
}
