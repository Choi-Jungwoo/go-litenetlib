package litenetlib_test

import (
	"encoding/binary"
	"net"
	"net/netip"
	"testing"
	"time"

	litenetlib "github.com/choi-jungwoo/go-litenetlib"
)

// fakeNtpServer answers one request with a reply built by reply(request).
func fakeNtpServer(t *testing.T, reply func(req []byte) []byte) netip.AddrPort {
	t.Helper()
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	go func() {
		buf := make([]byte, 512)
		n, from, err := conn.ReadFromUDPAddrPort(buf)
		if err == nil {
			conn.WriteToUDPAddrPort(reply(buf[:n]), from)
		}
	}()
	return netip.MustParseAddrPort(conn.LocalAddr().String())
}

const ntp2020 = uint64(3786825600) << 32 // 2020-01-01T00:00:00Z in NTP format

func ntpReply(req []byte, stratum byte) []byte {
	r := make([]byte, 48)
	r[0] = 0<<6 | 4<<3 | 4 // no leap warning, version 4, server mode
	r[1] = stratum
	copy(r[24:32], req[40:48]) // origin = client transmit
	binary.BigEndian.PutUint64(r[32:], ntp2020)
	binary.BigEndian.PutUint64(r[40:], ntp2020)
	return r
}

func requestNtp(t *testing.T, server netip.AddrPort) *litenetlib.NtpPacket {
	t.Helper()
	done := make(chan *litenetlib.NtpPacket, 1)
	req, err := litenetlib.NewNtpRequest(server, func(p *litenetlib.NtpPacket) { done <- p })
	if err != nil {
		t.Fatal(err)
	}
	defer req.Close()
	req.Send()
	select {
	case p := <-done:
		return p
	case <-time.After(2 * time.Second):
		t.Fatal("no NTP callback")
		return nil
	}
}

func TestNtpRequest(t *testing.T) {
	requests := make(chan []byte, 1)
	server := fakeNtpServer(t, func(req []byte) []byte {
		requests <- append([]byte(nil), req...)
		return ntpReply(req, 1)
	})
	p := requestNtp(t, server)
	if request := <-requests; len(request) != 48 || request[0] != 0<<6|4<<3|3 {
		t.Fatalf("request % x, want 48 bytes, version 4, client mode", request)
	}
	if p == nil {
		t.Fatal("valid reply rejected")
	}
	want := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	if got := p.ReceiveTimestamp(); !got.Equal(want) {
		t.Errorf("receive timestamp %v, want %v", got, want)
	}
	if p.Mode() != litenetlib.NtpModeServer || p.VersionNumber() != 4 || p.Stratum() != 1 {
		t.Errorf("header: mode %v version %d stratum %d", p.Mode(), p.VersionNumber(), p.Stratum())
	}
	// The server clock reads 2020, so the correction is the (negative) distance to it.
	offset, err := p.CorrectionOffset()
	if err != nil {
		t.Fatal(err)
	}
	if d := time.Until(want) - offset; d > time.Second || d < -time.Second {
		t.Errorf("correction offset %v, want about %v", offset, time.Until(want))
	}
}

func TestNtpRequestRejectsKissOfDeath(t *testing.T) {
	server := fakeNtpServer(t, func(req []byte) []byte { return ntpReply(req, 0) })
	if p := requestNtp(t, server); p != nil {
		t.Fatal("stratum 0 (kiss-o'-death) reply accepted")
	}
}
