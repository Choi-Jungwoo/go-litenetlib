package litenetlib

import (
	"log"
	"sync/atomic"
)

// NetLogLevel is the severity of a library log message (NetDebug.cs).
type NetLogLevel byte

const (
	NetLogWarning NetLogLevel = iota
	NetLogError
	NetLogTrace
	NetLogInfo
)

// NetLogger receives library log messages (INetLogger).
type NetLogger interface {
	WriteNet(level NetLogLevel, format string, args ...any)
}

type loggerHolder struct{ NetLogger }

var logger atomic.Pointer[loggerHolder]

// SetLogger routes library messages to l; nil restores the default (standard log package).
// Like the reference release build, only errors are written.
func SetLogger(l NetLogger) { logger.Store(&loggerHolder{l}) }

func writeError(format string, args ...any) {
	if h := logger.Load(); h != nil && h.NetLogger != nil {
		h.WriteNet(NetLogError, format, args...)
		return
	}
	log.Printf(format, args...)
}
