package litenetlib

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net"
	"net/netip"
	"time"
)

// NtpDefaultPort is the standard NTP port.
const NtpDefaultPort = 123

// NtpLeapIndicator warns of an impending leap second (Ntp/NtpPacket.cs).
type NtpLeapIndicator byte

const (
	NtpNoWarning NtpLeapIndicator = iota
	NtpLastMinuteHas61Seconds
	NtpLastMinuteHas59Seconds
	NtpAlarmCondition
)

// NtpMode is the SNTP packet mode.
type NtpMode byte

const (
	NtpModeClient NtpMode = 3
	NtpModeServer NtpMode = 4
)

var ntpEpoch = time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)

// NtpPacket is an SNTP request or reply. Missing timestamps are the zero time.
type NtpPacket struct {
	bytes       []byte
	destination time.Time
}

// NewNtpPacket returns a client request stamped with the current time.
func NewNtpPacket() *NtpPacket {
	p := &NtpPacket{bytes: make([]byte, 48)}
	p.bytes[0] = byte(4<<3) | byte(NtpModeClient)
	p.setDateTime64(40, time.Now())
	return p
}

// NtpPacketFromServerResponse wraps a reply received at destination.
func NtpPacketFromServerResponse(b []byte, destination time.Time) (*NtpPacket, error) {
	if len(b) < 48 {
		return nil, errors.New("litenetlib: SNTP reply packet must be at least 48 bytes long")
	}
	return &NtpPacket{bytes: b, destination: destination}, nil
}

func (p *NtpPacket) Bytes() []byte                   { return p.bytes }
func (p *NtpPacket) LeapIndicator() NtpLeapIndicator { return NtpLeapIndicator(p.bytes[0] & 0xC0 >> 6) }
func (p *NtpPacket) VersionNumber() int              { return int(p.bytes[0] & 0x38 >> 3) }
func (p *NtpPacket) Mode() NtpMode                   { return NtpMode(p.bytes[0] & 0x07) }
func (p *NtpPacket) Stratum() int                    { return int(p.bytes[1]) }
func (p *NtpPacket) Poll() int                       { return int(p.bytes[2]) }
func (p *NtpPacket) Precision() int                  { return int(int8(p.bytes[3])) }
func (p *NtpPacket) RootDelay() time.Duration        { return p.timeSpan32(4) }
func (p *NtpPacket) RootDispersion() time.Duration   { return p.timeSpan32(8) }
func (p *NtpPacket) ReferenceID() uint32             { return binary.BigEndian.Uint32(p.bytes[12:]) }
func (p *NtpPacket) ReferenceTimestamp() time.Time   { return p.dateTime64(16) }
func (p *NtpPacket) OriginTimestamp() time.Time      { return p.dateTime64(24) }
func (p *NtpPacket) ReceiveTimestamp() time.Time     { return p.dateTime64(32) }
func (p *NtpPacket) TransmitTimestamp() time.Time    { return p.dateTime64(40) }
func (p *NtpPacket) DestinationTimestamp() time.Time {
	return p.destination
}

// RoundTripTime is the network round trip of the request.
func (p *NtpPacket) RoundTripTime() (time.Duration, error) {
	if err := p.checkTimestamps(); err != nil {
		return 0, err
	}
	return p.ReceiveTimestamp().Sub(p.OriginTimestamp()) + p.destination.Sub(p.TransmitTimestamp()), nil
}

// CorrectionOffset is the amount to add to the local clock to match the server.
func (p *NtpPacket) CorrectionOffset() (time.Duration, error) {
	if err := p.checkTimestamps(); err != nil {
		return 0, err
	}
	d := p.ReceiveTimestamp().Sub(p.OriginTimestamp()) - p.destination.Sub(p.TransmitTimestamp())
	return d / 100 / 2 * 100, nil // TimeSpan.FromTicks(ticks / 2)
}

func (p *NtpPacket) validateReply() error {
	switch {
	case p.Mode() != NtpModeServer:
		return errors.New("this is not a reply SNTP packet")
	case p.VersionNumber() == 0:
		return errors.New("protocol version of the reply is not specified")
	case p.Stratum() == 0:
		return fmt.Errorf("received Kiss-o'-Death SNTP packet with code 0x%x", p.ReferenceID())
	case p.LeapIndicator() == NtpAlarmCondition:
		return errors.New("SNTP server has unsynchronized clock")
	}
	return p.checkTimestamps()
}

func (p *NtpPacket) checkTimestamps() error {
	switch {
	case p.OriginTimestamp().IsZero():
		return errors.New("origin timestamp is missing")
	case p.ReceiveTimestamp().IsZero():
		return errors.New("receive timestamp is missing")
	case p.TransmitTimestamp().IsZero():
		return errors.New("transmit timestamp is missing")
	case p.destination.IsZero():
		return errors.New("destination timestamp is missing")
	}
	return nil
}

func (p *NtpPacket) dateTime64(offset int) time.Time {
	field := binary.BigEndian.Uint64(p.bytes[offset:])
	if field == 0 {
		return time.Time{}
	}
	ticks := int64(math.RoundToEven(float64(field) * (1.0 / (1 << 32) * 10000000.0)))
	return ntpEpoch.Add(time.Duration(ticks) * 100)
}

func (p *NtpPacket) setDateTime64(offset int, t time.Time) {
	ticks := int64(t.Sub(ntpEpoch) / 100)
	binary.BigEndian.PutUint64(p.bytes[offset:], uint64(math.RoundToEven(float64(ticks)*(0.0000001*(1<<32)))))
}

func (p *NtpPacket) timeSpan32(offset int) time.Duration {
	seconds := float64(int32(binary.BigEndian.Uint32(p.bytes[offset:]))) / (1 << 16)
	return time.Duration(math.Round(seconds*1e7)) * 100
}

// NtpRequest queries an NTP server (Ntp/NtpRequest.cs): Send, wait for the callback,
// then Close. The callback runs on a library goroutine and gets nil on failure.
type NtpRequest struct {
	server     netip.AddrPort
	onComplete func(*NtpPacket)
	conn4      *net.UDPConn
	conn6      *net.UDPConn
	workers    group
}

// NewNtpRequest opens the sockets for querying server.
func NewNtpRequest(server netip.AddrPort, onComplete func(*NtpPacket)) (*NtpRequest, error) {
	r := &NtpRequest{server: netip.AddrPortFrom(server.Addr().Unmap(), server.Port()), onComplete: onComplete}
	var err error
	if r.conn4, err = listenUDP("udp4", netip.MustParseAddrPort("0.0.0.0:0"), false); err != nil {
		return nil, err
	}
	port := uint16(r.conn4.LocalAddr().(*net.UDPAddr).Port)
	r.conn6, _ = listenUDP("udp6", netip.AddrPortFrom(netip.IPv6Unspecified(), port), false)
	for _, c := range []*net.UDPConn{r.conn4, r.conn6} {
		if c != nil {
			r.workers.Go(func() { r.receive(c) })
		}
	}
	return r, nil
}

// NewNtpRequestHost resolves host and opens the sockets for querying it.
func NewNtpRequestHost(host string, port int, onComplete func(*NtpPacket)) (*NtpRequest, error) {
	server, err := MakeEndPoint(host, port)
	if err != nil {
		return nil, err
	}
	return NewNtpRequest(server, onComplete)
}

// Send sends a request; the callback gets nil if sending fails.
func (r *NtpRequest) Send() {
	data := NewNtpPacket().bytes
	conn := r.conn4
	if r.server.Addr().Is6() && r.conn6 != nil {
		conn = r.conn6
	}
	if n, err := conn.WriteToUDPAddrPort(data, r.server); err != nil || n != len(data) {
		r.onComplete(nil)
	}
}

// Close releases the sockets; call it after the reply or a timeout.
func (r *NtpRequest) Close() {
	r.conn4.Close()
	if r.conn6 != nil {
		r.conn6.Close()
	}
	r.workers.Wait()
}

func (r *NtpRequest) receive(conn *net.UDPConn) {
	buf := make([]byte, maxPacketSize)
	for {
		n, from, err := conn.ReadFromUDPAddrPort(buf)
		if errors.Is(err, net.ErrClosed) {
			return
		}
		destination := time.Now()
		if err != nil || netip.AddrPortFrom(from.Addr().Unmap(), from.Port()) != r.server {
			continue
		}
		if n < 48 {
			r.onComplete(nil)
			continue
		}
		p, _ := NtpPacketFromServerResponse(append([]byte(nil), buf[:n]...), destination)
		if p.validateReply() != nil {
			p = nil
		}
		r.onComplete(p)
	}
}
