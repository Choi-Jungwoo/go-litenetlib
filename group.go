package litenetlib

import (
	"bytes"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
)

// group runs goroutines that may call user code. Wait skips the calling goroutine, as the
// reference's NetSocket.Close skips joining Thread.CurrentThread, so a callback can stop
// the manager or close the request that runs it.
type group struct {
	mu      sync.Mutex
	members []*groupMember
}

type groupMember struct {
	id   atomic.Uint64
	done chan struct{}
}

func (g *group) Go(f func()) {
	gm := &groupMember{done: make(chan struct{})}
	g.mu.Lock()
	g.members = append(g.members, gm)
	g.mu.Unlock()
	go func() {
		defer close(gm.done)
		gm.id.Store(goid())
		f()
	}()
}

// Wait waits for all goroutines except the caller, then forgets the finished ones and
// the caller, as the reference drops its thread references after the joins.
func (g *group) Wait() {
	g.mu.Lock()
	members := slices.Clone(g.members)
	g.mu.Unlock()
	self := goid()
	for _, gm := range members {
		if gm.id.Load() != self {
			<-gm.done
		}
	}
	g.mu.Lock()
	g.members = slices.DeleteFunc(g.members, func(gm *groupMember) bool {
		if gm.id.Load() == self {
			return true
		}
		select {
		case <-gm.done:
			return true
		default:
			return false
		}
	})
	g.mu.Unlock()
}

// goid parses the current goroutine id from the stack header ("goroutine 7 [running]:").
// It runs once per goroutine start and per Wait, never on a hot path.
func goid() uint64 {
	var buf [32]byte
	b := bytes.TrimPrefix(buf[:runtime.Stack(buf[:], false)], []byte("goroutine "))
	id, _ := strconv.ParseUint(string(b[:bytes.IndexByte(b, ' ')]), 10, 64)
	return id
}
