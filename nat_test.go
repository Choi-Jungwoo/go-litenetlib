package litenetlib_test

import (
	"bytes"
	"net/netip"
	"sync"
	"testing"
	"time"

	litenetlib "github.com/choi-jungwoo/go-litenetlib"
)

// TestInteropNatMasterServer pairs two .NET peers by token, like a master server.
func TestInteropNatMasterServer(t *testing.T) {
	type request struct{ local, remote netip.AddrPort }
	var mu sync.Mutex
	waiting := map[string]request{}
	introduced := make(chan struct{}, 1)
	var m *litenetlib.Manager
	nat := &litenetlib.EventBasedNatPunchListener{
		NatIntroductionRequest: func(local, remote netip.AddrPort, token string) {
			mu.Lock()
			defer mu.Unlock()
			host, ok := waiting[token]
			if !ok {
				waiting[token] = request{local, remote}
				return
			}
			m.NatPunchModule.NatIntroduce(host.local, host.remote, local, remote, token)
			introduced <- struct{}{}
		},
	}
	startServer(t, &litenetlib.EventBasedListener{}, func(mm *litenetlib.Manager) {
		m = mm
		m.NatPunchEnabled = true
		m.NatPunchModule.Init(nat)
	})
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			case <-time.After(5 * time.Millisecond):
				m.NatPunchModule.PollEvents()
			}
		}
	}()
	defer close(done)

	runClient(t, "nat", m.LocalPort())

	select {
	case <-introduced:
	default:
		t.Fatal("master never introduced the peers")
	}
}

// TestRawNatHost checks the host side: an introduction triggers punches to both
// endpoints, and a received punch raises a success event.
func TestRawNatHost(t *testing.T) {
	success := make(chan string, 1)
	var m *litenetlib.Manager
	startServer(t, &litenetlib.EventBasedListener{}, func(mm *litenetlib.Manager) {
		m = mm
		m.NatPunchEnabled = true
		m.NatPunchModule.Init(&litenetlib.EventBasedNatPunchListener{
			NatIntroductionSuccess: func(target netip.AddrPort, token string) { success <- target.String() + " " + token },
		})
	})
	internal := newRawClient(t, m.LocalPort())
	external := newRawClient(t, m.LocalPort())

	w := litenetlib.NewDataWriter()
	w.PutByte(13) // NatIntroduction
	w.PutByte(1)  // we are the host
	w.PutEndPoint(netip.MustParseAddrPort(internal.conn.LocalAddr().String()))
	w.PutEndPoint(netip.MustParseAddrPort(external.conn.LocalAddr().String()))
	w.PutString("tok")
	internal.send(w.Data()...)

	want := []byte{14, 1, 3, 0, 0, 0, 't', 'o', 'k'} // NatPunchMessage, host byte, token
	for _, c := range []*rawClient{internal, external} {
		if d := c.recv(time.Second); !bytes.Equal(d, want) {
			t.Fatalf("punch % x, want % x", d, want)
		}
	}

	external.send(14, 0, 3, 0, 0, 0, 't', 'o', 'k')
	deadline := time.After(time.Second)
	for {
		m.NatPunchModule.PollEvents()
		select {
		case s := <-success:
			if want := external.conn.LocalAddr().String() + " tok"; s != want {
				t.Fatalf("success %q, want %q", s, want)
			}
			return
		case <-deadline:
			t.Fatal("no punch success event")
		case <-time.After(5 * time.Millisecond):
		}
	}
}
