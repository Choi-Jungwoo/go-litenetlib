package litenetlib_test

import (
	"testing"

	litenetlib "github.com/choi-jungwoo/go-litenetlib"
)

// TestReaderStopsAtLogicalEnd: a short packet in a larger pooled buffer must not read the
// stale bytes past its end.
func TestReaderStopsAtLogicalEnd(t *testing.T) {
	buf := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	r := litenetlib.NewDataReader(buf[:2])
	defer func() {
		if recover() == nil {
			t.Fatal("GetInt read past the end of the data")
		}
	}()
	r.GetInt()
}

// TestPutStringMaxInvalidUTF8 encodes invalid UTF-8 as U+FFFD like a .NET string would be.
func TestPutStringMaxInvalidUTF8(t *testing.T) {
	w := litenetlib.NewDataWriter()
	w.PutStringMax("a\xff", 10)
	if got := litenetlib.NewDataReader(w.Data()).GetString(); got != "a�" {
		t.Fatalf("got %q", got)
	}
}
