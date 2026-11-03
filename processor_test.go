package litenetlib_test

import (
	"slices"
	"testing"

	litenetlib "github.com/choi-jungwoo/go-litenetlib"
)

type color uint8

type vec struct{ X, Y float32 }

func (v *vec) Serialize(w *litenetlib.DataWriter) { w.PutFloat(v.X); w.PutFloat(v.Y) }
func (v *vec) Deserialize(r *litenetlib.DataReader) {
	v.X = r.GetFloat()
	v.Y = r.GetFloat()
}

// samplePacket mirrors Interop.SamplePacket property by property.
type samplePacket struct {
	ID      int32
	Name    string
	Color   color
	Flag    bool
	Blob    []byte
	Numbers []int32
	Tags    []string
	Pos     vec
	Path    []vec
	Big     int64
	D       float64
	C       uint16
}

type ping struct{ Seq int32 }

func (p *ping) Serialize(w *litenetlib.DataWriter)   { w.PutInt(p.Seq) }
func (p *ping) Deserialize(r *litenetlib.DataReader) { p.Seq = r.GetInt() }

func TestInteropPacketProcessor(t *testing.T) {
	pp := litenetlib.NewPacketProcessor(0)
	var replies []func(w *litenetlib.DataWriter) error
	err := litenetlib.Subscribe(pp, "Interop.SamplePacket", func(p *samplePacket, _ any) {
		want := samplePacket{77, "naïve 🌍", 2, true, []byte{9, 8}, []int32{1, -2}, []string{"x", "yz"},
			vec{1.5, -2}, []vec{{3, 4}}, -5000000000, 0.25, 'Ж'}
		if p.ID != want.ID || p.Name != want.Name || p.Color != want.Color || !p.Flag ||
			!slices.Equal(p.Blob, want.Blob) || !slices.Equal(p.Numbers, want.Numbers) ||
			!slices.Equal(p.Tags, want.Tags) || p.Pos != want.Pos || !slices.Equal(p.Path, want.Path) ||
			p.Big != want.Big || p.D != want.D || p.C != want.C {
			t.Errorf("got %+v, want %+v", *p, want)
		}
		replies = append(replies, func(w *litenetlib.DataWriter) error {
			return litenetlib.Write(pp, w, "Interop.SamplePacket", p)
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	litenetlib.SubscribeNetSerializable(pp, "Interop.Ping", func(p *ping, _ any) {
		replies = append(replies, func(w *litenetlib.DataWriter) error {
			pp.WriteNetSerializable(w, "Interop.Ping", &ping{p.Seq + 1})
			return nil
		})
	})

	m := startServer(t, &litenetlib.EventBasedListener{
		ConnectionRequest: acceptKey,
		NetworkReceive: func(peer *litenetlib.Peer, r *litenetlib.PacketReader, method litenetlib.DeliveryMethod) {
			defer r.Recycle()
			replies = replies[:0]
			if err := pp.ReadAllPackets(&r.DataReader, nil); err != nil {
				t.Error(err)
			}
			w := litenetlib.NewDataWriter()
			for _, reply := range replies {
				if err := reply(w); err != nil {
					t.Error(err)
				}
			}
			peer.Send(w.Data(), method)
		},
	}, nil)
	runClient(t, "packet_processor", m.LocalPort())
}

func TestPacketProcessorRejectsUnknownPackets(t *testing.T) {
	pp := litenetlib.NewPacketProcessor(0)
	w := litenetlib.NewDataWriter()
	w.PutULong(litenetlib.PacketHash("Nope"))
	if err := pp.ReadPacket(litenetlib.NewDataReader(w.Data()), nil); err == nil {
		t.Fatal("unknown packet hash accepted")
	}
}

func TestPacketProcessorRejectsUnsupportedFields(t *testing.T) {
	type bad struct{ N int }
	if err := litenetlib.Subscribe(litenetlib.NewPacketProcessor(0), "Bad", func(*bad, any) {}); err == nil {
		t.Fatal("platform-sized int accepted; it has no C# counterpart")
	}
}

func TestPacketHash(t *testing.T) {
	// FNV hash over UTF-16 code units of the type name, as NetPacketProcessor.GetHash.
	// Expected value computed independently: offset ^ 'A' * prime.
	if got, want := litenetlib.PacketHash("A"), uint64(0xaf63fc4c860222ec); got != want {
		t.Fatalf("PacketHash(\"A\") = %#x, want %#x", got, want)
	}
}

func TestPacketProcessorSend(t *testing.T) {
	type score struct{ Points int32 }
	pp := litenetlib.NewPacketProcessor(0)
	var m *litenetlib.Manager
	startServer(t, &litenetlib.EventBasedListener{
		ConnectionRequest: acceptKey,
		PeerConnected: func(p *litenetlib.Peer) {
			if err := litenetlib.Send(pp, p, "Game.Score", &score{7}, litenetlib.Unreliable); err != nil {
				t.Error(err)
			}
			if err := pp.SendNetSerializableToAll(m, "Interop.Ping", &ping{9}, litenetlib.Unreliable); err != nil {
				t.Error(err)
			}
		},
	}, func(mm *litenetlib.Manager) { m = mm })
	c := newRawClient(t, m.LocalPort())
	c.send(connectRequest(7, 1, 0, "key")...)
	c.expect(pConnectAccept)

	for _, want := range []struct {
		name  string
		value int32
	}{{"Game.Score", 7}, {"Interop.Ping", 9}} {
		w := litenetlib.NewDataWriter()
		w.PutByte(pUnreliable)
		w.PutULong(litenetlib.PacketHash(want.name))
		w.PutInt(want.value)
		if got := c.expect(pUnreliable); string(got) != string(w.Data()) {
			t.Fatalf("%s: got % x, want % x", want.name, got, w.Data())
		}
	}
}
