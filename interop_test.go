package litenetlib_test

import (
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	litenetlib "github.com/choi-jungwoo/go-litenetlib"
)

var (
	buildOnce sync.Once
	buildErr  error
	clientDLL string
)

// runClient runs one scenario of the .NET v0.8.3 interop client against port.
func runClient(t *testing.T, scenario string, port int, args ...string) {
	t.Helper()
	if _, err := exec.LookPath("dotnet"); err != nil {
		t.Skip("dotnet not installed")
	}
	buildOnce.Do(func() {
		out := filepath.Join("interop", "bin")
		cmd := exec.Command("dotnet", "build", filepath.Join("interop", "client"), "-c", "Release", "-o", out, "-nologo", "-v", "q")
		cmd.Stderr = os.Stderr
		if b, err := cmd.Output(); err != nil {
			buildErr = &buildError{err, string(b)}
		}
		clientDLL = filepath.Join(out, "client.dll")
	})
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	args = append([]string{clientDLL, scenario, strconv.Itoa(port)}, args...)
	out, err := exec.Command("dotnet", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("client %s: %v\n%s", scenario, err, out)
	}
}

type buildError struct {
	err error
	out string
}

func (e *buildError) Error() string { return e.err.Error() + "\n" + e.out }

// startServer starts a manager on a random port and polls its events until the test ends.
func startServer(t *testing.T, l litenetlib.EventListener, configure func(*litenetlib.Manager)) *litenetlib.Manager {
	t.Helper()
	m := litenetlib.NewManager(l)
	if configure != nil {
		configure(m)
	}
	if err := m.Start(0); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		for {
			select {
			case <-done:
				return
			case <-time.After(5 * time.Millisecond):
				m.PollEvents()
			}
		}
	}()
	t.Cleanup(func() {
		close(done)
		<-stopped
		m.Stop(true)
	})
	return m
}

func acceptKey(r *litenetlib.ConnectionRequest) { r.AcceptIfKey("key") }

func TestInteropConnect(t *testing.T) {
	connected := make(chan *litenetlib.Peer, 1)
	m := startServer(t, &litenetlib.EventBasedListener{
		ConnectionRequest: acceptKey,
		PeerConnected:     func(p *litenetlib.Peer) { connected <- p },
	}, nil)

	runClient(t, "connect", m.LocalPort())

	select {
	case p := <-connected:
		if !p.EndPoint().Addr().IsLoopback() {
			t.Errorf("peer endpoint %v, want loopback", p.EndPoint())
		}
	default:
		t.Fatal("server never reported the peer as connected")
	}
}

func TestInteropClientDisconnect(t *testing.T) {
	type result struct {
		reason litenetlib.DisconnectReason
		data   []byte
	}
	disconnected := make(chan result, 1)
	m := startServer(t, &litenetlib.EventBasedListener{
		ConnectionRequest: acceptKey,
		PeerDisconnected: func(p *litenetlib.Peer, info litenetlib.DisconnectInfo) {
			disconnected <- result{info.Reason, info.AdditionalData.GetRemainingBytes()}
		},
	}, nil)

	runClient(t, "client_disconnect", m.LocalPort())

	select {
	case r := <-disconnected:
		if r.reason != litenetlib.RemoteConnectionClose || string(r.data) != "\x01\x02\x03" {
			t.Fatalf("got %v %v, want RemoteConnectionClose [1 2 3]", r.reason, r.data)
		}
	case <-time.After(time.Second):
		t.Fatal("no disconnect event")
	}
}

func TestInteropServerDisconnect(t *testing.T) {
	peers := make(chan *litenetlib.Peer, 1)
	m := startServer(t, &litenetlib.EventBasedListener{
		ConnectionRequest: acceptKey,
		PeerConnected: func(p *litenetlib.Peer) {
			p.Disconnect([]byte{9, 8, 7})
			peers <- p
		},
	}, nil)

	runClient(t, "server_disconnect", m.LocalPort())

	p := <-peers
	if s := p.ConnectionState(); s != litenetlib.StateDisconnected {
		t.Fatalf("server peer state %v, want Disconnected after ShutdownOk", s)
	}
}

func TestInteropReject(t *testing.T) {
	m := startServer(t, &litenetlib.EventBasedListener{
		ConnectionRequest: func(r *litenetlib.ConnectionRequest) { r.Reject([]byte{5, 5}) },
		PeerConnected:     func(*litenetlib.Peer) { t.Error("rejected peer connected") },
	}, nil)
	runClient(t, "reject", m.LocalPort())
}

func TestInteropWrongKey(t *testing.T) {
	results := make(chan litenetlib.ConnectionRequestResult, 1)
	m := startServer(t, &litenetlib.EventBasedListener{
		ConnectionRequest: func(r *litenetlib.ConnectionRequest) {
			if r.AcceptIfKey("key") != nil {
				t.Error("AcceptIfKey accepted a wrong key")
			}
			results <- r.Result()
		},
	}, nil)
	runClient(t, "wrong_key", m.LocalPort())
	if r := <-results; r != litenetlib.RequestReject {
		t.Fatalf("result %v, want RequestReject", r)
	}
}

func echoServer(t *testing.T) *litenetlib.Manager {
	return startServer(t, &litenetlib.EventBasedListener{
		ConnectionRequest: acceptKey,
		NetworkReceive: func(p *litenetlib.Peer, r *litenetlib.PacketReader, method litenetlib.DeliveryMethod) {
			if err := p.Send(r.GetRemainingBytes(), method); err != nil {
				t.Error(err)
			}
			r.Recycle()
		},
	}, nil)
}

func TestInteropEcho(t *testing.T) {
	runClient(t, "echo", echoServer(t).LocalPort())
}

func TestInteropEchoThroughLossyNetwork(t *testing.T) {
	proxy := startLossyProxy(t, echoServer(t).LocalPort())
	runClient(t, "echo", proxy, "lossy")
}

func TestInteropMtuAndPing(t *testing.T) {
	var mu sync.Mutex
	var peer *litenetlib.Peer
	latencyUpdates := 0
	m := startServer(t, &litenetlib.EventBasedListener{
		ConnectionRequest: acceptKey,
		PeerConnected: func(p *litenetlib.Peer) {
			mu.Lock()
			peer = p
			mu.Unlock()
		},
		NetworkLatencyUpdate: func(*litenetlib.Peer, int) {
			mu.Lock()
			latencyUpdates++
			mu.Unlock()
		},
	}, nil)

	runClient(t, "mtu_ping", m.LocalPort())

	mu.Lock()
	defer mu.Unlock()
	if peer.Mtu() != 1432 {
		t.Errorf("server MTU %d, want 1432", peer.Mtu())
	}
	if latencyUpdates < 2 {
		t.Errorf("%d latency updates, want at least 2", latencyUpdates)
	}
	if d := peer.RemoteTimeDelta(); d > 10_000_000 || d < -10_000_000 {
		t.Errorf("remote time delta %d ticks, want under 1s", d)
	}
}

func TestInteropTimeout(t *testing.T) {
	reasons := make(chan litenetlib.DisconnectReason, 1)
	m := startServer(t, &litenetlib.EventBasedListener{
		ConnectionRequest: acceptKey,
		PeerDisconnected: func(_ *litenetlib.Peer, info litenetlib.DisconnectInfo) {
			reasons <- info.Reason
		},
	}, func(m *litenetlib.Manager) { m.DisconnectTimeout = 300 })

	runClient(t, "vanish", m.LocalPort())

	select {
	case r := <-reasons:
		if r != litenetlib.Timeout {
			t.Fatalf("reason %v, want Timeout", r)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no timeout disconnect")
	}
	deadline := time.Now().Add(time.Second)
	for m.PeersCountInState(litenetlib.StateAny|litenetlib.StateDisconnected) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("timed out peer was never removed")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if m.PeersCount() != 0 {
		t.Fatalf("PeersCount %d, want 0", m.PeersCount())
	}
}

func TestInteropUnconnectedAndDiscoveryResponse(t *testing.T) {
	var m *litenetlib.Manager
	startServer(t, &litenetlib.EventBasedListener{
		NetworkReceiveUnconnected: func(addr netip.AddrPort, r *litenetlib.PacketReader, typ litenetlib.UnconnectedMessageType) {
			switch s := r.GetString(); {
			case typ == litenetlib.BasicMessage && s == "ping":
				m.SendUnconnectedMessage(str("pong"), addr)
			case typ == litenetlib.BasicMessage && s == "who":
				m.SendDiscoveryResponse(str("me"), addr)
			default:
				t.Errorf("unexpected %v message %q", typ, s)
			}
			r.Recycle()
		},
	}, func(mm *litenetlib.Manager) {
		m = mm
		m.UnconnectedMessagesEnabled = true
	})
	runClient(t, "unconnected", m.LocalPort())
}

func str(s string) []byte {
	w := litenetlib.NewDataWriter()
	w.PutString(s)
	return w.Data()
}

func TestInteropSerialization(t *testing.T) {
	m := startServer(t, &litenetlib.EventBasedListener{
		ConnectionRequest: acceptKey,
		NetworkReceive: func(p *litenetlib.Peer, r *litenetlib.PacketReader, _ litenetlib.DeliveryMethod) {
			defer r.Recycle()
			check := func(ok bool, what string) {
				if !ok {
					t.Errorf("read %s", what)
				}
			}
			check(r.GetByte() == 200, "byte")
			check(r.GetSByte() == -5, "sbyte")
			check(r.GetBool(), "bool")
			check(r.GetChar() == 'Ж', "char")
			check(r.GetUShort() == 65000, "ushort")
			check(r.GetShort() == -1234, "short")
			check(r.GetInt() == -123456789, "int")
			check(r.GetUInt() == 4000000000, "uint")
			check(r.GetLong() == -1234567890123, "long")
			check(r.GetULong() == 18000000000000000000, "ulong")
			check(r.GetFloat() == 3.25, "float")
			check(r.GetDouble() == -1.5e100, "double")
			check(r.GetString() == "héllo wörld 🌍", "string")
			check(r.GetStringMax(10) == "héllo", "string max")
			check(r.GetString() == "", "empty string")
			check(string(r.GetBytesWithLength()) == "\x01\x02\x03", "bytes with length")
			check(slices.Equal(r.GetIntArray(), []int32{1, -2, 3}), "int array")
			check(slices.Equal(r.GetStringArray(), []string{"a", "bc"}), "string array")
			check(slices.Equal(r.GetBoolArray(), []bool{true, false}), "bool array")
			check(slices.Equal(r.GetFloatArray(), []float32{1.5}), "float array")
			ep, err := r.GetNetEndPoint()
			check(err == nil && ep.String() == "10.1.2.3:4567", "endpoint")
			check(r.EndOfData(), "end of data")

			w := litenetlib.NewDataWriter()
			w.PutByte(200)
			w.PutSByte(-5)
			w.PutBool(true)
			w.PutChar('Ж')
			w.PutUShort(65000)
			w.PutShort(-1234)
			w.PutInt(-123456789)
			w.PutUInt(4000000000)
			w.PutLong(-1234567890123)
			w.PutULong(18000000000000000000)
			w.PutFloat(3.25)
			w.PutDouble(-1.5e100)
			w.PutString("héllo wörld 🌍")
			w.PutStringMax("héllo", 10)
			w.PutString("")
			w.PutBytesWithLength([]byte{1, 2, 3})
			w.PutIntArray([]int32{1, -2, 3})
			w.PutStringArray([]string{"a", "bc"})
			w.PutBoolArray([]bool{true, false})
			w.PutFloatArray([]float32{1.5})
			w.PutEndPoint(netip.MustParseAddrPort("10.1.2.3:4567"))
			if err := p.Send(w.Data(), litenetlib.ReliableOrdered); err != nil {
				t.Error(err)
			}
		},
	}, nil)
	runClient(t, "serialize", m.LocalPort())
}

func TestInteropReconnect(t *testing.T) {
	var mu sync.Mutex
	var events []string
	record := func(s string) {
		mu.Lock()
		events = append(events, s)
		mu.Unlock()
	}
	m := startServer(t, &litenetlib.EventBasedListener{
		ConnectionRequest: acceptKey,
		PeerConnected:     func(*litenetlib.Peer) { record("connected") },
		PeerDisconnected: func(_ *litenetlib.Peer, info litenetlib.DisconnectInfo) {
			record("disconnected:" + strconv.Itoa(int(info.Reason)))
		},
		NetworkReceive: func(p *litenetlib.Peer, r *litenetlib.PacketReader, method litenetlib.DeliveryMethod) {
			p.Send(r.GetRemainingBytes(), method)
			r.Recycle()
		},
	}, nil)

	runClient(t, "reconnect", m.LocalPort())

	mu.Lock()
	defer mu.Unlock()
	closed := "disconnected:" + strconv.Itoa(int(litenetlib.RemoteConnectionClose))
	want := []string{"connected", closed, "connected", closed, "connected"}
	if !slices.Equal(events[:min(len(events), len(want))], want) {
		t.Fatalf("events %v, want prefix %v", events, want)
	}
}
