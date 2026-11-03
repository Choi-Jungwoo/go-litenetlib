package litenetlib_test

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	litenetlib "github.com/choi-jungwoo/go-litenetlib"
)

type captureLogger struct {
	mu    sync.Mutex
	lines []string
}

func (l *captureLogger) WriteNet(level litenetlib.NetLogLevel, format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf("%d %s", level, fmt.Sprintf(format, args...)))
}

func (l *captureLogger) waitFor(t *testing.T, line string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		l.mu.Lock()
		for _, s := range l.lines {
			if strings.HasPrefix(s, line) {
				l.mu.Unlock()
				return
			}
		}
		l.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("log line %q not written; got %q", line, l.lines)
}

func TestLoggerReportsErrors(t *testing.T) {
	log := &captureLogger{}
	litenetlib.SetLogger(log)
	defer litenetlib.SetLogger(nil)
	m := startServer(t, &litenetlib.EventBasedListener{ConnectionRequest: acceptKey}, nil)
	c := newRawClient(t, m.LocalPort())

	c.send(pPing, 1) // shorter than the ping header
	log.waitFor(t, fmt.Sprintf("%d [NM] DataReceived: bad!", litenetlib.NetLogError))

	c.send(connectRequest(7, 1, 0, "key")...)
	c.expect(pConnectAccept)
	probe := make([]byte, 508)
	probe[0] = 15 // MtuCheck whose embedded sizes do not match its length
	c.send(probe...)
	log.waitFor(t, fmt.Sprintf("%d [MTU] Broken packet. RMTU 0, EMTU 0, PSIZE 508", litenetlib.NetLogError))
}

// reentrantLogger triggers another library log from inside WriteNet, as a logger that itself
// uses the library might.
type reentrantLogger struct {
	port   int
	nested chan struct{}
}

func (l *reentrantLogger) WriteNet(litenetlib.NetLogLevel, string, ...any) {
	select {
	case <-l.nested:
		return
	default:
		close(l.nested)
	}
	litenetlib.NewManager(&litenetlib.EventBasedListener{}).Start(l.port) // logs a bind error
}

// TestLoggerIsReentrant: like a C# lock, the logger lock lets WriteNet log again.
func TestLoggerIsReentrant(t *testing.T) {
	m := startServer(t, &litenetlib.EventBasedListener{}, nil)
	l := &reentrantLogger{port: m.LocalPort(), nested: make(chan struct{})}
	litenetlib.SetLogger(l)
	defer litenetlib.SetLogger(nil)
	done := make(chan struct{})
	go func() {
		litenetlib.NewManager(&litenetlib.EventBasedListener{}).Start(m.LocalPort())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("nested library log deadlocked")
	}
}
