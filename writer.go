package litenetlib

import (
	"encoding/binary"
	"math"
	"net/netip"
	"strings"
	"unicode/utf8"
)

// DataWriter writes values in the NetDataWriter format (Utils/NetDataWriter.cs).
// The zero value is ready to use; Reset reuses its buffer.
type DataWriter struct {
	data []byte
}

// NewDataWriter returns an empty writer.
func NewDataWriter() *DataWriter { return &DataWriter{data: make([]byte, 0, 64)} }

// Data returns the written bytes; they are valid until the next write or Reset.
func (w *DataWriter) Data() []byte { return w.data }

// Length is the number of written bytes.
func (w *DataWriter) Length() int { return len(w.data) }

// Reset empties the writer, keeping its buffer.
func (w *DataWriter) Reset() { w.data = w.data[:0] }

// CopyData returns a copy of the written bytes.
func (w *DataWriter) CopyData() []byte { return append([]byte{}, w.data...) }

func (w *DataWriter) PutByte(v byte)     { w.data = append(w.data, v) }
func (w *DataWriter) PutSByte(v int8)    { w.data = append(w.data, byte(v)) }
func (w *DataWriter) PutChar(v uint16)   { w.PutUShort(v) }
func (w *DataWriter) PutUShort(v uint16) { w.data = binary.LittleEndian.AppendUint16(w.data, v) }
func (w *DataWriter) PutShort(v int16)   { w.PutUShort(uint16(v)) }
func (w *DataWriter) PutUInt(v uint32)   { w.data = binary.LittleEndian.AppendUint32(w.data, v) }
func (w *DataWriter) PutInt(v int32)     { w.PutUInt(uint32(v)) }
func (w *DataWriter) PutULong(v uint64)  { w.data = binary.LittleEndian.AppendUint64(w.data, v) }
func (w *DataWriter) PutLong(v int64)    { w.PutULong(uint64(v)) }
func (w *DataWriter) PutFloat(v float32) { w.PutUInt(math.Float32bits(v)) }
func (w *DataWriter) PutDouble(v float64) {
	w.PutULong(math.Float64bits(v))
}

func (w *DataWriter) PutBool(v bool) {
	if v {
		w.PutByte(1)
	} else {
		w.PutByte(0)
	}
}

// PutBytes writes raw bytes.
func (w *DataWriter) PutBytes(v []byte) { w.data = append(w.data, v...) }

// PutBytesWithLength writes an int32 length followed by v.
func (w *DataWriter) PutBytesWithLength(v []byte) {
	w.PutInt(int32(len(v)))
	w.PutBytes(v)
}

// PutString writes an int32 UTF-8 byte count followed by the bytes.
func (w *DataWriter) PutString(v string) {
	w.PutInt(int32(len(v)))
	w.data = append(w.data, v...)
}

// PutStringMax writes at most maxLength UTF-16 code units of v. Like the reference, the
// length prefix and the space reserved are those of the whole string. The reference leaves
// whatever the buffer held in the unused space; this port writes zeros there.
func (w *DataWriter) PutStringMax(v string, maxLength int) {
	if v == "" {
		w.PutInt(0)
		return
	}
	// A .NET string always encodes to valid UTF-8; mirror its U+FFFD replacement so the
	// encoded prefix never outgrows len(v).
	v = strings.ToValidUTF8(v, string(utf8.RuneError))
	w.PutInt(int32(len(v)))
	start := len(w.data)
	units := 0
	for _, c := range v {
		n := 1
		if c > 0xFFFF {
			n = 2
		}
		if units+n > maxLength {
			if units < maxLength {
				w.data = utf8.AppendRune(w.data, utf8.RuneError) // split surrogate pair
			}
			break
		}
		units += n
		w.data = utf8.AppendRune(w.data, c)
	}
	w.data = append(w.data, make([]byte, len(v)-(len(w.data)-start))...)
}

// PutEndPoint writes the address as a string followed by the int32 port.
func (w *DataWriter) PutEndPoint(v netip.AddrPort) {
	w.PutString(v.Addr().String())
	w.PutInt(int32(v.Port()))
}

func putArray[T any](w *DataWriter, v []T, put func(T)) {
	w.PutUShort(uint16(len(v)))
	for _, x := range v[:uint16(len(v))] {
		put(x)
	}
}

func (w *DataWriter) PutBoolArray(v []bool)      { putArray(w, v, w.PutBool) }
func (w *DataWriter) PutUShortArray(v []uint16)  { putArray(w, v, w.PutUShort) }
func (w *DataWriter) PutShortArray(v []int16)    { putArray(w, v, w.PutShort) }
func (w *DataWriter) PutIntArray(v []int32)      { putArray(w, v, w.PutInt) }
func (w *DataWriter) PutUIntArray(v []uint32)    { putArray(w, v, w.PutUInt) }
func (w *DataWriter) PutLongArray(v []int64)     { putArray(w, v, w.PutLong) }
func (w *DataWriter) PutULongArray(v []uint64)   { putArray(w, v, w.PutULong) }
func (w *DataWriter) PutFloatArray(v []float32)  { putArray(w, v, w.PutFloat) }
func (w *DataWriter) PutDoubleArray(v []float64) { putArray(w, v, w.PutDouble) }
func (w *DataWriter) PutStringArray(v []string)  { putArray(w, v, w.PutString) }
func (w *DataWriter) PutStringArrayMax(v []string, maxLength int) {
	putArray(w, v, func(s string) { w.PutStringMax(s, maxLength) })
}
