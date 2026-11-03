package litenetlib

import (
	"net/netip"
	"sync"
)

// NatPunchListener receives NAT punch events from NatPunchModule.PollEvents.
type NatPunchListener interface {
	OnNatIntroductionRequest(localEndPoint, remoteEndPoint netip.AddrPort, token string)
	OnNatIntroductionSuccess(targetEndPoint netip.AddrPort, token string)
}

// EventBasedNatPunchListener adapts optional callbacks to NatPunchListener.
type EventBasedNatPunchListener struct {
	NatIntroductionRequest func(localEndPoint, remoteEndPoint netip.AddrPort, token string)
	NatIntroductionSuccess func(targetEndPoint netip.AddrPort, token string)
}

func (l *EventBasedNatPunchListener) OnNatIntroductionRequest(localEndPoint, remoteEndPoint netip.AddrPort, token string) {
	if l.NatIntroductionRequest != nil {
		l.NatIntroductionRequest(localEndPoint, remoteEndPoint, token)
	}
}

func (l *EventBasedNatPunchListener) OnNatIntroductionSuccess(targetEndPoint netip.AddrPort, token string) {
	if l.NatIntroductionSuccess != nil {
		l.NatIntroductionSuccess(targetEndPoint, token)
	}
}

const (
	natHostByte   = 1
	natClientByte = 0
	// MaxTokenLength is the maximum NAT punch token length in UTF-16 code units.
	MaxTokenLength = 256
)

type natRequestEvent struct {
	local, remote netip.AddrPort
	token         string
}

type natSuccessEvent struct {
	target netip.AddrPort
	token  string
}

// NatPunchModule performs UDP NAT hole punching (NatPunchModule.cs). As a master server it
// introduces peers with NatIntroduce; as a host it punches when introduced.
// Messages are only processed when Manager.NatPunchEnabled is set.
type NatPunchModule struct {
	m        *Manager
	mu       sync.Mutex
	requests []natRequestEvent
	success  []natSuccessEvent
	listener NatPunchListener
}

// Init sets the listener that PollEvents delivers to.
func (n *NatPunchModule) Init(listener NatPunchListener) {
	n.mu.Lock()
	n.listener = listener
	n.mu.Unlock()
}

// NatIntroduce tells a host and a client about each other so they punch through their NATs.
func (n *NatPunchModule) NatIntroduce(hostInternal, hostExternal, clientInternal, clientExternal netip.AddrPort, additionalInfo string) {
	w := NewDataWriter()
	w.PutByte(byte(propNatIntroduction))
	w.PutByte(natClientByte)
	w.PutEndPoint(hostInternal)
	w.PutEndPoint(hostExternal)
	w.PutStringMax(additionalInfo, MaxTokenLength)
	n.m.writeTo(w.Data(), clientExternal)

	w.Reset()
	w.PutByte(byte(propNatIntroduction))
	w.PutByte(natHostByte)
	w.PutEndPoint(clientInternal)
	w.PutEndPoint(clientExternal)
	w.PutStringMax(additionalInfo, MaxTokenLength)
	n.m.writeTo(w.Data(), hostExternal)
}

// SendNatIntroduceRequest registers this manager with a master server under a token.
func (n *NatPunchModule) SendNatIntroduceRequest(masterServer netip.AddrPort, additionalInfo string) {
	w := NewDataWriter()
	w.PutByte(byte(propNatIntroductionRequest))
	networkIP := GetLocalIP(LocalAddrIPv4)
	if networkIP == "" {
		networkIP = GetLocalIP(LocalAddrIPv6)
	}
	local, err := MakeEndPoint(networkIP, n.m.LocalPort())
	if err != nil {
		return
	}
	w.PutEndPoint(local)
	w.PutStringMax(additionalInfo, MaxTokenLength)
	n.m.writeTo(w.Data(), masterServer)
}

// PollEvents delivers queued NAT events (success first, then requests, as in the reference).
func (n *NatPunchModule) PollEvents() {
	n.mu.Lock()
	l := n.listener
	if l == nil {
		n.mu.Unlock()
		return
	}
	success, requests := n.success, n.requests
	n.success, n.requests = nil, nil
	n.mu.Unlock()
	for _, e := range success {
		l.OnNatIntroductionSuccess(e.target, e.token)
	}
	for _, e := range requests {
		l.OnNatIntroductionRequest(e.local, e.remote, e.token)
	}
}

func (n *NatPunchModule) processMessage(sender netip.AddrPort, pk *packet) {
	r := &DataReader{}
	r.setSource(pk.data, HeaderSize, len(pk.data))
	switch pk.property() {
	case propNatIntroductionRequest:
		local, err := r.GetNetEndPoint()
		if err != nil {
			return
		}
		token := r.GetStringMax(MaxTokenLength)
		n.mu.Lock()
		n.requests = append(n.requests, natRequestEvent{local, sender, token})
		n.mu.Unlock()

	case propNatIntroduction:
		hostByte := r.GetByte()
		remoteInternal, err1 := r.GetNetEndPoint()
		remoteExternal, err2 := r.GetNetEndPoint()
		if err1 != nil || err2 != nil {
			return
		}
		token := r.GetStringMax(MaxTokenLength)
		w := NewDataWriter()
		w.PutByte(byte(propNatPunchMessage))
		w.PutByte(hostByte)
		w.PutString(token)
		n.m.writeTo(w.Data(), remoteInternal) // internal punch
		if hostByte == natHostByte {
			n.m.setTTL(2)
			n.m.writeTo(w.Data(), remoteExternal) // external punch
			n.m.setTTL(SocketTTL)
		} else {
			n.m.writeTo(w.Data(), remoteExternal)
		}

	case propNatPunchMessage:
		if b := r.GetByte(); b != natHostByte && b != natClientByte {
			return // garbage
		}
		token := r.GetStringMax(MaxTokenLength)
		n.mu.Lock()
		n.success = append(n.success, natSuccessEvent{sender, token})
		n.mu.Unlock()
	}
}
