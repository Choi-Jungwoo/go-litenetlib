package litenetlib

import (
	"net/netip"
	"sync"
)

type eventType byte

const (
	evConnect eventType = iota
	evDisconnect
	evReceive
	evReceiveUnconnected
	evError
	evLatencyUpdated
	evDiscoveryRequest
	evDiscoveryResponse
	evConnectionRequest
)

type netEvent struct {
	typ     eventType
	peer    *Peer
	addr    netip.AddrPort
	latency int
	err     error
	reason  DisconnectReason
	request *ConnectionRequest
	method  DeliveryMethod
	reader  PacketReader
}

// PacketReader is a DataReader over a received packet. Call Recycle when done with it
// unless Manager.AutoRecycle is set; it must not be used after recycling.
type PacketReader struct {
	DataReader
	packet *packet
	m      *Manager
	evt    *netEvent
}

// Recycle returns the packet and the event to their pools.
func (r *PacketReader) Recycle() {
	r.Clear()
	if r.packet != nil {
		r.m.pool.recycle(r.packet)
	}
	r.packet = nil
	r.m.recycleEvent(r.evt)
}

var eventPool = sync.Pool{New: func() any {
	e := &netEvent{}
	e.reader.evt = e
	return e
}}

func (m *Manager) newEvent(typ eventType, peer *Peer, addr netip.AddrPort, err error) *netEvent {
	e := eventPool.Get().(*netEvent)
	e.typ, e.peer, e.addr, e.err = typ, peer, addr, err
	e.reader.m = m
	return e
}

func (e *netEvent) setSource(pk *packet) {
	if pk == nil {
		return
	}
	e.reader.packet = pk
	e.reader.setSource(pk.data, pk.headerSize(), len(pk.data))
}

func (m *Manager) recycleEvent(e *netEvent) {
	*e = netEvent{reader: PacketReader{evt: e}}
	eventPool.Put(e)
}

func (m *Manager) pushEvent(e *netEvent) {
	m.eventsMu.Lock()
	m.events = append(m.events, e)
	m.eventsMu.Unlock()
	if m.UnsyncedEvents {
		select {
		case m.eventSignal <- struct{}{}:
		default:
		}
	}
}

func (m *Manager) popEvent() *netEvent {
	m.eventsMu.Lock()
	defer m.eventsMu.Unlock()
	if m.eventsHead == len(m.events) {
		return nil
	}
	e := m.events[m.eventsHead]
	m.events[m.eventsHead] = nil
	m.eventsHead++
	m.events, m.eventsHead = compact(m.events, m.eventsHead)
	return e
}

// PollEvents delivers all pending events to the listener. It does nothing with UnsyncedEvents.
func (m *Manager) PollEvents() {
	if m.UnsyncedEvents {
		return
	}
	for e := m.popEvent(); e != nil; e = m.popEvent() {
		m.processEvent(e)
	}
}

// dispatchEvents serves one Start..Stop session. A callback may stop and restart the
// manager, so it checks its own session's stop channel after every event and never
// touches the next session's queue.
func (m *Manager) dispatchEvents(stop, signal <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		case <-signal:
		}
		for e := m.popEvent(); e != nil; e = m.popEvent() {
			m.processUnsynced(e)
			select {
			case <-stop:
				return
			default:
			}
		}
	}
}

// processUnsynced runs an event like the reference runs unsynced callbacks: inside the
// receive thread's catch block, so a panicking callback is logged and dispatch goes on.
func (m *Manager) processUnsynced(e *netEvent) {
	defer func() {
		if r := recover(); r != nil {
			writeError("[NM] SocketReceiveThread error: %v", r)
		}
	}()
	m.processEvent(e)
}

func (m *Manager) processEvent(e *netEvent) {
	emptyData := e.reader.IsNull()
	l := m.listener
	switch e.typ {
	case evConnect:
		l.OnPeerConnected(e.peer)
	case evDisconnect:
		l.OnPeerDisconnected(e.peer, DisconnectInfo{Reason: e.reason, SocketError: e.err, AdditionalData: &e.reader})
	case evReceive:
		l.OnNetworkReceive(e.peer, &e.reader, e.method)
	case evReceiveUnconnected:
		l.OnNetworkReceiveUnconnected(e.addr, &e.reader, BasicMessage)
	case evDiscoveryRequest:
		l.OnNetworkReceiveUnconnected(e.addr, &e.reader, DiscoveryRequest)
	case evDiscoveryResponse:
		l.OnNetworkReceiveUnconnected(e.addr, &e.reader, DiscoveryResponse)
	case evError:
		l.OnNetworkError(e.addr, e.err)
	case evLatencyUpdated:
		l.OnNetworkLatencyUpdate(e.peer, e.latency)
	case evConnectionRequest:
		l.OnConnectionRequest(e.request)
	}
	if emptyData {
		m.recycleEvent(e)
	} else if m.AutoRecycle {
		e.reader.Recycle()
	}
}
