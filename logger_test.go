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
