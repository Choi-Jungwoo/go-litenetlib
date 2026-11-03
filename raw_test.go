package litenetlib_test

import (
	"bytes"
	"encoding/binary"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	litenetlib "github.com/choi-jungwoo/go-litenetlib"
)

// Packet property ids from NetPacket.cs v0.8.3.
const (
	pUnreliable        = 0
	pSequenced         = 2
	pReliableOrdered   = 3
	pAckReliableOrder  = 5
	pPing              = 6
	pPong              = 7
	pConnectRequest    = 8
	pConnectAccept     = 9
	pDisconnect        = 10
	pUnconnected       = 11
	pDiscoveryRequest  = 17
	pDiscoveryResponse = 18
	pMerged            = 19
	pShutdownOk        = 20
	pPeerNotFound      = 23
	pReliableSequenced = 21
	pInvalidProtocol   = 24
)

// rawClient speaks the wire protocol by hand.
type rawClient struct {
	t       *testing.T
	conn    *net.UDPConn
	server  netip.AddrPort
	pending [][]byte // packets split out of merged datagrams
}

func newRawClient(t *testing.T, port int) *rawClient {
	t.Helper()
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return &rawClient{t: t, conn: conn, server: netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(port))}
}

func (c *rawClient) send(b ...byte) {
	c.t.Helper()
	if _, err := c.conn.WriteToUDPAddrPort(b, c.server); err != nil {
		c.t.Fatal(err)
	}
}

// recv returns the next datagram, or nil after timeout.
func (c *rawClient) recv(timeout time.Duration) []byte {
	buf := make([]byte, 2048)
	c.conn.SetReadDeadline(time.Now().Add(timeout))
	n, _, err := c.conn.ReadFromUDPAddrPort(buf)
	if err != nil {
		return nil
	}
	return buf[:n]
}

// expect returns the next packet with property prop, splitting merged datagrams and
// skipping other packets (pings, MTU probes).
func (c *rawClient) expect(prop byte) []byte {
	c.t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		for len(c.pending) > 0 {
			p := c.pending[0]
			c.pending = c.pending[1:]
			if p[0]&0x1F == prop {
				return p
			}
		}
		d := c.recv(time.Until(deadline))
		if d == nil {
			c.t.Fatalf("no packet with property %d", prop)
		}
		if d[0]&0x1F != pMerged {
			c.pending = append(c.pending, d)
			continue
		}
		for pos := 1; pos+2 <= len(d); {
			n := int(binary.LittleEndian.Uint16(d[pos:]))
			c.pending = append(c.pending, d[pos+2:pos+2+n])
			pos += 2 + n
		}
	}
}

// expectNothing asserts the server sends no datagram for a while.
func (c *rawClient) expectNothing() {
	c.t.Helper()
	if d := c.recv(200 * time.Millisecond); d != nil {
		c.t.Fatalf("unexpected datagram % x", d)
	}
}

func connectRequest(protocol int32, connectTime int64, num byte, key string) []byte {
	b := []byte{pConnectRequest | num<<5}
	b = binary.LittleEndian.AppendUint32(b, uint32(protocol))
	b = binary.LittleEndian.AppendUint64(b, uint64(connectTime))
	b = binary.LittleEndian.AppendUint32(b, uint32(len(key)))
	return append(b, key...)
}

func TestRawInvalidProtocol(t *testing.T) {
	m := startServer(t, &litenetlib.EventBasedListener{}, nil)
	c := newRawClient(t, m.LocalPort())
	c.send(connectRequest(6, 1, 0, "key")...)
	if d := c.recv(time.Second); !bytes.Equal(d, []byte{pInvalidProtocol}) {
		t.Fatalf("got % x, want InvalidProtocol [18]", d)
	}
}

func TestRawUnknownPeer(t *testing.T) {
	m := startServer(t, &litenetlib.EventBasedListener{}, nil)
	c := newRawClient(t, m.LocalPort())

	c.send(pUnreliable, 1, 2, 3)
	if d := c.recv(time.Second); !bytes.Equal(d, []byte{pPeerNotFound}) {
		t.Fatalf("data from unknown peer: got % x, want PeerNotFound", d)
	}

	c.send(pDisconnect, 1, 2, 3, 4, 5, 6, 7, 8)
	if d := c.recv(time.Second); !bytes.Equal(d, []byte{pShutdownOk}) {
		t.Fatalf("disconnect from unknown peer: got % x, want ShutdownOk", d)
	}

	// The PeerNotFound handshake request (flag 0) is echoed back with flag 1.
	c.send(pPeerNotFound, 0, 1, 2, 3, 4, 5, 6, 7, 8)
	if d := c.recv(time.Second); !bytes.Equal(d, []byte{pPeerNotFound, 1, 1, 2, 3, 4, 5, 6, 7, 8}) {
		t.Fatalf("PeerNotFound echo: got % x", d)
	}

	// Malformed packets (shorter than their header) are dropped silently.
	c.send(pPing, 1)
	c.expectNothing()
}

func TestRawConnectAccept(t *testing.T) {
	connected := make(chan *litenetlib.Peer, 4)
	m := startServer(t, &litenetlib.EventBasedListener{
		ConnectionRequest: acceptKey,
		PeerConnected:     func(p *litenetlib.Peer) { connected <- p },
	}, nil)
	c := newRawClient(t, m.LocalPort())

	c.send(connectRequest(7, 0x0102030405060708, 2, "key")...)
	want := []byte{pConnectAccept, 8, 7, 6, 5, 4, 3, 2, 1, 2, 0}
	if got := c.expect(pConnectAccept); !bytes.Equal(got, want) {
		t.Fatalf("accept % x, want % x", got, want)
	}
	<-connected

	// A repeated request with the same connect time just repeats the accept.
	c.send(connectRequest(7, 0x0102030405060708, 2, "key")...)
	if got := c.expect(pConnectAccept); !bytes.Equal(got, want) {
		t.Fatalf("repeated accept % x, want % x", got, want)
	}
	select {
	case <-connected:
		t.Fatal("repeated request created a second connection")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestRawPingPong(t *testing.T) {
	m := startServer(t, &litenetlib.EventBasedListener{ConnectionRequest: acceptKey}, nil)
	c := newRawClient(t, m.LocalPort())
	c.send(connectRequest(7, 42, 1, "key")...)
	c.expect(pConnectAccept)

	c.send(pPing|1<<5, 0x34, 0x12)
	pong := c.expect(pPong)
	if len(pong) != 11 || pong[0] != pPong|1<<5 || binary.LittleEndian.Uint16(pong[1:]) != 0x1234 {
		t.Fatalf("pong % x", pong)
	}
	ticks := int64(binary.LittleEndian.Uint64(pong[3:]))
	now := time.Now().UnixNano()/100 + 621355968000000000 // DateTime.UtcNow.Ticks
	if d := now - ticks; d < 0 || d > 10_000_000 {
		t.Fatalf("pong time %d is %d ticks from now", ticks, d)
	}

	// An older ping is not answered.
	c.send(pPing|1<<5, 0x33, 0x12)
	c.send(pPing|1<<5, 0x35, 0x12)
	if pong := c.expect(pPong); binary.LittleEndian.Uint16(pong[1:]) != 0x1235 {
		t.Fatalf("answered stale ping: % x", pong)
	}

	// Packets with another connection number are ignored.
	c.send(pPing|2<<5, 0x40, 0x12)
	c.send(pPing|1<<5, 0x41, 0x12)
	if pong := c.expect(pPong); binary.LittleEndian.Uint16(pong[1:]) != 0x1241 {
		t.Fatalf("answered ping with wrong connection number: % x", pong)
	}
}

func TestRawReliableOrderedAcksAndReordering(t *testing.T) {
	got := make(chan string, 8)
	m := startServer(t, &litenetlib.EventBasedListener{
		ConnectionRequest: acceptKey,
		NetworkReceive: func(_ *litenetlib.Peer, r *litenetlib.PacketReader, method litenetlib.DeliveryMethod) {
			if method != litenetlib.ReliableOrdered {
				t.Errorf("method %v", method)
			}
			got <- string(r.GetRemainingBytes())
			r.Recycle()
		},
	}, nil)
	c := newRawClient(t, m.LocalPort())
	c.send(connectRequest(7, 42, 0, "key")...)
	c.expect(pConnectAccept)

	// Sequence 1 arrives before 0, merged into one datagram with a duplicate of 1.
	c.send(pMerged,
		4, 0, pReliableOrdered, 1, 0, 'b',
		4, 0, pReliableOrdered, 1, 0, 'b',
		4, 0, pReliableOrdered, 0, 0, 'a')
	for _, want := range []string{"a", "b"} {
		select {
		case s := <-got:
			if s != want {
				t.Fatalf("got %q, want %q", s, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("missing %q", want)
		}
	}
	// Ack: window start 0, bits 0 and 1 set in an 8-byte (64-bit) window.
	want := []byte{pAckReliableOrder, 0, 0, 0b11, 0, 0, 0, 0, 0, 0, 0}
	if ack := c.expect(pAckReliableOrder); !bytes.Equal(ack, want) {
		t.Fatalf("ack % x, want % x", ack, want)
	}
	select {
	case s := <-got:
		t.Fatalf("duplicate delivered: %q", s)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestRawDiscoveryRequest(t *testing.T) {
	requests := make(chan string, 2)
	var m *litenetlib.Manager
	startServer(t, &litenetlib.EventBasedListener{
		NetworkReceiveUnconnected: func(addr netip.AddrPort, r *litenetlib.PacketReader, typ litenetlib.UnconnectedMessageType) {
			if typ == litenetlib.DiscoveryRequest {
				requests <- string(r.GetRemainingBytes())
				m.SendDiscoveryResponse([]byte("here"), addr)
			}
			r.Recycle()
		},
	}, func(mm *litenetlib.Manager) { m = mm; m.DiscoveryEnabled = true })
	c := newRawClient(t, m.LocalPort())

	c.send(pDiscoveryRequest, 'w', 'h', 'o')
	if d := c.recv(time.Second); !bytes.Equal(d, []byte{pDiscoveryResponse, 'h', 'e', 'r', 'e'}) {
		t.Fatalf("discovery response % x", d)
	}
	if s := <-requests; s != "who" {
		t.Fatalf("request data %q", s)
	}
}

func TestRawUnsyncedEventsDisconnectFromCallback(t *testing.T) {
	m := litenetlib.NewManager(&litenetlib.EventBasedListener{
		ConnectionRequest: acceptKey,
		PeerConnected:     func(p *litenetlib.Peer) { p.Disconnect([]byte{0xAB}) },
	})
	m.UnsyncedEvents = true
	if err := m.Start(0); err != nil {
		t.Fatal(err)
	}
	defer m.Stop(true)
	c := newRawClient(t, m.LocalPort())

	c.send(connectRequest(7, 0x1122334455667788, 3, "key")...)
	c.expect(pConnectAccept)
	want := []byte{pDisconnect | 3<<5, 0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, 0xAB}
	if got := c.expect(pDisconnect); !bytes.Equal(got, want) {
		t.Fatalf("disconnect % x, want % x", got, want)
	}
	// The disconnect is resent every 300ms until ShutdownOk arrives.
	if got := c.expect(pDisconnect); !bytes.Equal(got, want) {
		t.Fatalf("resent disconnect % x", got)
	}
	c.send(pShutdownOk)
	deadline := time.Now().Add(time.Second)
	for m.PeersCountInState(litenetlib.StateDisconnected) != 1 {
		if time.Now().After(deadline) {
			t.Fatal("ShutdownOk did not complete the disconnect")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestRawUnsyncedCallbackPanicIsRecovered(t *testing.T) {
	got := make(chan byte, 2)
	m := litenetlib.NewManager(&litenetlib.EventBasedListener{
		NetworkReceiveUnconnected: func(_ netip.AddrPort, r *litenetlib.PacketReader, _ litenetlib.UnconnectedMessageType) {
			b := r.GetByte()
			if b == 1 {
				panic("callback bug")
			}
			got <- b
		},
	})
	m.UnsyncedEvents = true
	m.UnconnectedMessagesEnabled = true
	if err := m.Start(0); err != nil {
		t.Fatal(err)
	}
	defer m.Stop(true)
	c := newRawClient(t, m.LocalPort())
	c.send(pUnconnected, 1)
	c.send(pUnconnected, 2)
	select {
	case b := <-got:
		if b != 2 {
			t.Fatalf("got message %d, want 2", b)
		}
	case <-time.After(time.Second):
		t.Fatal("event after the panicking callback was not delivered")
	}
}

func TestRawStopFromUnsyncedCallback(t *testing.T) {
	stopped := make(chan struct{})
	var m *litenetlib.Manager
	m = litenetlib.NewManager(&litenetlib.EventBasedListener{
		NetworkReceiveUnconnected: func(netip.AddrPort, *litenetlib.PacketReader, litenetlib.UnconnectedMessageType) {
			m.Stop(true)
			close(stopped)
		},
	})
	m.UnsyncedEvents = true
	m.UnconnectedMessagesEnabled = true
	if err := m.Start(0); err != nil {
		t.Fatal(err)
	}
	newRawClient(t, m.LocalPort()).send(pUnconnected)
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop called from a callback did not return")
	}
	if m.IsRunning() {
		t.Fatal("manager still running")
	}
}

// TestRawStopFromCallbackDuringStop: a callback calling Stop while another Stop waits for
// that callback returns at once instead of deadlocking.
func TestRawStopFromCallbackDuringStop(t *testing.T) {
	entered := make(chan struct{})
	var m *litenetlib.Manager
	m = litenetlib.NewManager(&litenetlib.EventBasedListener{
		NetworkReceiveUnconnected: func(netip.AddrPort, *litenetlib.PacketReader, litenetlib.UnconnectedMessageType) {
			close(entered)
			for m.IsRunning() { // wait for the outer Stop
				time.Sleep(time.Millisecond)
			}
			m.Stop(true)
		},
	})
	m.UnsyncedEvents = true
	m.UnconnectedMessagesEnabled = true
	if err := m.Start(0); err != nil {
		t.Fatal(err)
	}
	newRawClient(t, m.LocalPort()).send(pUnconnected)
	<-entered
	stopped := make(chan struct{})
	go func() {
		m.Stop(true)
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop deadlocked with a callback calling Stop")
	}
}

// TestRawInconsistentFragmentsDropped: parts that do not tile the result are dropped, and a
// later well-formed fragmented packet is still delivered.
func TestRawInconsistentFragmentsDropped(t *testing.T) {
	c, got := connectedReceiver(t)

	seq := uint16(0)
	sendFragments := func(id uint16, parts ...string) {
		for i, part := range parts {
			c.send(fragment(pReliableOrdered, seq, id, uint16(i), uint16(len(parts)), part)...)
			seq++
		}
	}
	sendFragments(1, "abcde", "f", "g")
	sendFragments(2, "ab", "cd", "e")
	select {
	case s := <-got:
		if s != "abcde" {
			t.Fatalf("got %q, want the well-formed packet", s)
		}
	case <-time.After(time.Second):
		t.Fatal("well-formed fragmented packet not delivered")
	}
}

func TestRawRestart(t *testing.T) {
	m := litenetlib.NewManager(&litenetlib.EventBasedListener{})
	if err := m.Start(0); err != nil {
		t.Fatal(err)
	}
	if err := m.Start(0); err != litenetlib.ErrAlreadyRunning {
		t.Fatalf("second Start: %v, want ErrAlreadyRunning", err)
	}
	m.Stop(true)
	if err := m.Start(0); err != nil {
		t.Fatal(err)
	}
	defer m.Stop(true)
	c := newRawClient(t, m.LocalPort())
	c.send(pUnreliable)
	if d := c.recv(time.Second); !bytes.Equal(d, []byte{pPeerNotFound}) {
		t.Fatalf("restarted manager replied % x", d)
	}
}

func TestRawRestartFromUnsyncedCallback(t *testing.T) {
	const messages = 50
	restarted := make(chan int, 1)
	received := make(chan struct{}, messages)
	var inCallback, overlaps atomic.Int32
	var m *litenetlib.Manager
	m = litenetlib.NewManager(&litenetlib.EventBasedListener{
		NetworkReceiveUnconnected: func(_ netip.AddrPort, r *litenetlib.PacketReader, _ litenetlib.UnconnectedMessageType) {
			if r.GetByte() == 0 { // first session: restart from inside the callback
				m.Stop(true)
				if err := m.Start(0); err != nil {
					t.Error(err)
				}
				restarted <- m.LocalPort()
				return
			}
			if inCallback.Add(1) > 1 {
				overlaps.Add(1)
			}
			time.Sleep(time.Millisecond)
			inCallback.Add(-1)
			received <- struct{}{}
		},
	})
	m.UnsyncedEvents = true
	m.UnconnectedMessagesEnabled = true
	if err := m.Start(0); err != nil {
		t.Fatal(err)
	}
	defer m.Stop(true)
	newRawClient(t, m.LocalPort()).send(pUnconnected, 0)
	c := newRawClient(t, <-restarted)
	for range messages {
		c.send(pUnconnected, 1)
	}
	for range messages {
		select {
		case <-received:
		case <-time.After(2 * time.Second):
			t.Fatal("messages of the restarted session were lost")
		}
	}
	if n := overlaps.Load(); n > 0 {
		t.Fatalf("%d callbacks ran concurrently with another one", n)
	}
}

func TestRawStopDoesNotWaitForPreviousSessionCallback(t *testing.T) {
	restarted := make(chan int, 1)
	newSessionStopped := make(chan struct{})
	var m *litenetlib.Manager
	m = litenetlib.NewManager(&litenetlib.EventBasedListener{
		NetworkReceiveUnconnected: func(_ netip.AddrPort, r *litenetlib.PacketReader, _ litenetlib.UnconnectedMessageType) {
			if r.GetByte() == 0 { // first session: restart, then wait for the new session's callback
				m.Stop(true)
				if err := m.Start(0); err != nil {
					t.Error(err)
				}
				restarted <- m.LocalPort()
				select {
				case <-newSessionStopped:
				case <-time.After(3 * time.Second):
				}
				return
			}
			m.Stop(true)
			close(newSessionStopped)
		},
	})
	m.UnsyncedEvents = true
	m.UnconnectedMessagesEnabled = true
	if err := m.Start(0); err != nil {
		t.Fatal(err)
	}
	defer m.Stop(true)
	newRawClient(t, m.LocalPort()).send(pUnconnected, 0)
	newRawClient(t, <-restarted).send(pUnconnected, 1)
	select {
	case <-newSessionStopped:
	case <-time.After(time.Second):
		t.Fatal("Stop in the new session waited for the previous session's callback")
	}
}

func TestRawSendDuringRestart(t *testing.T) {
	m := litenetlib.NewManager(&litenetlib.EventBasedListener{})
	if err := m.Start(0); err != nil {
		t.Fatal(err)
	}
	c := newRawClient(t, m.LocalPort())
	to := c.conn.LocalAddr().(*net.UDPAddr).AddrPort()
	var stop atomic.Bool
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for !stop.Load() {
				m.SendUnconnectedMessage([]byte{1}, to)
			}
		})
	}
	for range 200 {
		m.Stop(true)
		if err := m.Start(0); err != nil {
			t.Fatal(err)
		}
	}
	stop.Store(true)
	wg.Wait()
	m.Stop(true)
}

func TestRawSmallAPI(t *testing.T) {
	connected := make(chan *litenetlib.Peer, 1)
	m := litenetlib.NewManager(&litenetlib.EventBasedListener{
		ConnectionRequest: func(r *litenetlib.ConnectionRequest) {
			r.AcceptIfKey("key")
		},
		PeerConnected: func(p *litenetlib.Peer) { connected <- p },
	})
	m.UnsyncedEvents = true
	if err := m.StartHost("127.0.0.1", "::1", 0); err != nil {
		t.Fatal(err)
	}
	defer m.Stop(true)
	if m.FirstPeer() != nil {
		t.Fatal("FirstPeer of an empty manager")
	}
	c := newRawClient(t, m.LocalPort())
	c.send(connectRequest(7, 1, 0, "key")...)
	p := <-connected
	if m.FirstPeer() != p {
		t.Errorf("FirstPeer %v, want the connected peer", m.FirstPeer())
	}

	checkStatistics(t, m, p, c)

	s := litenetlib.Statistics{PacketsSent: 4, PacketsReceived: 3, BytesSent: 20, BytesReceived: 10, PacketLoss: 1}
	want := "BytesReceived: 10\nPacketsReceived: 3\nBytesSent: 20\nPacketsSent: 4\nPacketLoss: 1\nPacketLossPercent: 25\n"
	if s.String() != want {
		t.Errorf("String() = %q, want %q", s.String(), want)
	}
}

// connectedReceiver connects a raw client to a new server that sends every received
// payload to got.
func connectedReceiver(t *testing.T) (*rawClient, <-chan string) {
	t.Helper()
	got := make(chan string, 4)
	m := startServer(t, &litenetlib.EventBasedListener{
		ConnectionRequest: acceptKey,
		NetworkReceive: func(_ *litenetlib.Peer, r *litenetlib.PacketReader, _ litenetlib.DeliveryMethod) {
			got <- string(r.GetRemainingBytes())
			r.Recycle()
		},
	}, nil)
	c := newRawClient(t, m.LocalPort())
	c.send(connectRequest(7, 42, 0, "key")...)
	c.expect(pConnectAccept)
	return c, got
}

// fragment builds a fragmented sequenced-header packet (NetPacket layout).
func fragment(prop byte, seq, id, part, total uint16, payload string) []byte {
	b := []byte{prop | 0x80}
	for _, v := range []uint16{seq, id, part, total} {
		b = binary.LittleEndian.AppendUint16(b, v)
	}
	return append(b, payload...)
}

// TestRawZeroTotalFragmentDoesNotPoisonID: a fragment claiming zero parts is dropped and its
// fragment id still carries a later well-formed packet.
func TestRawZeroTotalFragmentDoesNotPoisonID(t *testing.T) {
	c, got := connectedReceiver(t)

	c.send(fragment(pReliableOrdered, 0, 5, 0, 0, "x")...)
	c.send(fragment(pReliableOrdered, 1, 5, 0, 2, "ab")...)
	c.send(fragment(pReliableOrdered, 2, 5, 1, 2, "c")...)
	select {
	case s := <-got:
		if s != "abc" {
			t.Fatalf("got %q, want %q", s, "abc")
		}
	case <-time.After(time.Second):
		t.Fatal("well-formed packet on a previously zero-total fragment id not delivered")
	}
}

// TestRawUnreliableFragmentsDropped: the reference client only fragments reliable unordered
// and ordered packets (NetPeer.Send), so fragments on the other channels are never delivered.
func TestRawUnreliableFragmentsDropped(t *testing.T) {
	for _, tc := range []struct {
		name string
		prop byte
	}{{"Unreliable", pUnreliable}, {"Sequenced", pSequenced}, {"ReliableSequenced", pReliableSequenced}} {
		t.Run(tc.name, func(t *testing.T) {
			c, got := connectedReceiver(t)

			c.send(fragment(tc.prop, 1, 9, 0, 2, "ab")...)
			c.send(fragment(tc.prop, 2, 9, 1, 2, "c")...)
			plain := []byte{tc.prop}
			if tc.prop != pUnreliable {
				plain = binary.LittleEndian.AppendUint16(plain, 3)
			}
			c.send(append(plain, "ok"...)...)
			select {
			case s := <-got:
				if s != "ok" {
					t.Fatalf("got %q, want only the unfragmented %q", s, "ok")
				}
			case <-time.After(time.Second):
				t.Fatal("unfragmented packet not delivered")
			}
		})
	}
}
