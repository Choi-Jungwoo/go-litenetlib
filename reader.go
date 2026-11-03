package litenetlib

import (
	"encoding/binary"
	"math"
	"net/netip"
)

// DataReader reads values written by a NetDataWriter (Utils/NetDataReader.cs).
// Like the reference, reading past the end panics; use the TryGet methods for untrusted input.
type DataReader struct {
	data     []byte
	position int
	dataSize int
	offset   int
}

// NewDataReader returns a reader over source.
func NewDataReader(source []byte) *DataReader {
	r := &DataReader{}
	r.SetSource(source)
	return r
}

// SetSource resets the reader to read all of source.
func (r *DataReader) SetSource(source []byte) { r.setSource(source, 0, len(source)) }

func (r *DataReader) setSource(source []byte, offset, size int) {
	r.data = source
	r.position = offset
	r.offset = offset
	r.dataSize = size
}

// RawData returns the underlying buffer.
func (r *DataReader) RawData() []byte { return r.data }

// RawDataSize is the end offset of readable data in RawData.
func (r *DataReader) RawDataSize() int { return r.dataSize }

// UserDataOffset is the offset of user data in RawData.
func (r *DataReader) UserDataOffset() int { return r.offset }

// UserDataSize is the size of user data.
func (r *DataReader) UserDataSize() int { return r.dataSize - r.offset }

// IsNull reports whether the reader has no source.
func (r *DataReader) IsNull() bool { return r.data == nil }

// Position is the current read offset in RawData.
func (r *DataReader) Position() int { return r.position }

// EndOfData reports whether all data was read.
func (r *DataReader) EndOfData() bool { return r.position == r.dataSize }

// AvailableBytes is the number of unread bytes.
func (r *DataReader) AvailableBytes() int { return r.dataSize - r.position }

// Clear detaches the reader from its source.
func (r *DataReader) Clear() { *r = DataReader{} }

// next and peek cap the slice at dataSize so a short packet in a pooled buffer cannot
// read stale bytes beyond its end.
func (r *DataReader) next(n int) []byte {
	b := r.data[r.position : r.position+n : r.dataSize]
	r.position += n
	return b
}

func (r *DataReader) GetByte() byte     { return r.next(1)[0] }
func (r *DataReader) GetSByte() int8    { return int8(r.GetByte()) }
func (r *DataReader) GetBool() bool     { return r.GetByte() > 0 }
func (r *DataReader) GetChar() uint16   { return r.GetUShort() }
func (r *DataReader) GetUShort() uint16 { return binary.LittleEndian.Uint16(r.next(2)) }
func (r *DataReader) GetShort() int16   { return int16(r.GetUShort()) }
func (r *DataReader) GetUInt() uint32   { return binary.LittleEndian.Uint32(r.next(4)) }
func (r *DataReader) GetInt() int32     { return int32(r.GetUInt()) }
func (r *DataReader) GetULong() uint64  { return binary.LittleEndian.Uint64(r.next(8)) }
func (r *DataReader) GetLong() int64    { return int64(r.GetULong()) }
func (r *DataReader) GetFloat() float32 { return math.Float32frombits(r.GetUInt()) }
func (r *DataReader) GetDouble() float64 {
	return math.Float64frombits(r.GetULong())
}

// GetString reads an int32 byte count followed by UTF-8 bytes.
func (r *DataReader) GetString() string {
	n := int(r.GetInt())
	if n <= 0 {
		return ""
	}
	return string(r.next(n))
}

// GetStringMax reads a string of at most maxLength UTF-16 code units. Longer strings
// yield "" and, as in the reference, leave the position right after the length prefix.
func (r *DataReader) GetStringMax(maxLength int) string {
	n := int(r.GetInt())
	if n <= 0 || n > maxLength*2 {
		return ""
	}
	s := string(r.data[r.position : r.position+n : r.dataSize])
	if utf16Len(s) > maxLength {
		return ""
	}
	r.position += n
	return s
}

// GetNetEndPoint reads an address string and an int32 port, resolving host names.
func (r *DataReader) GetNetEndPoint() (netip.AddrPort, error) {
	host := r.GetStringMax(1000)
	port := r.GetInt()
	return MakeEndPoint(host, int(port))
}

// GetBytes fills dst from the stream.
func (r *DataReader) GetBytes(dst []byte) { copy(dst, r.next(len(dst))) }

// GetBytesWithLength reads an int32 length followed by that many bytes.
func (r *DataReader) GetBytesWithLength() []byte {
	n := int(r.GetInt())
	return append([]byte{}, r.next(n)...)
}

// GetRemainingBytes returns a copy of the unread bytes and moves to the end.
func (r *DataReader) GetRemainingBytes() []byte {
	out := make([]byte, r.AvailableBytes())
	copy(out, r.data[r.position:])
	r.position = len(r.data)
	return out
}

func getArray[T any](r *DataReader, get func() T) []T {
	arr := make([]T, r.GetUShort())
	for i := range arr {
		arr[i] = get()
	}
	return arr
}

func (r *DataReader) GetBoolArray() []bool      { return getArray(r, r.GetBool) }
func (r *DataReader) GetUShortArray() []uint16  { return getArray(r, r.GetUShort) }
func (r *DataReader) GetShortArray() []int16    { return getArray(r, r.GetShort) }
func (r *DataReader) GetIntArray() []int32      { return getArray(r, r.GetInt) }
func (r *DataReader) GetUIntArray() []uint32    { return getArray(r, r.GetUInt) }
func (r *DataReader) GetLongArray() []int64     { return getArray(r, r.GetLong) }
func (r *DataReader) GetULongArray() []uint64   { return getArray(r, r.GetULong) }
func (r *DataReader) GetFloatArray() []float32  { return getArray(r, r.GetFloat) }
func (r *DataReader) GetDoubleArray() []float64 { return getArray(r, r.GetDouble) }
func (r *DataReader) GetStringArray() []string  { return getArray(r, r.GetString) }
func (r *DataReader) GetStringArrayMax(maxLength int) []string {
	return getArray(r, func() string { return r.GetStringMax(maxLength) })
}

func (r *DataReader) peek(n int) []byte { return r.data[r.position : r.position+n : r.dataSize] }

func (r *DataReader) PeekByte() byte     { return r.peek(1)[0] }
func (r *DataReader) PeekSByte() int8    { return int8(r.PeekByte()) }
func (r *DataReader) PeekBool() bool     { return r.PeekByte() > 0 }
func (r *DataReader) PeekChar() uint16   { return r.PeekUShort() }
func (r *DataReader) PeekUShort() uint16 { return binary.LittleEndian.Uint16(r.peek(2)) }
func (r *DataReader) PeekShort() int16   { return int16(r.PeekUShort()) }
func (r *DataReader) PeekUInt() uint32   { return binary.LittleEndian.Uint32(r.peek(4)) }
func (r *DataReader) PeekInt() int32     { return int32(r.PeekUInt()) }
func (r *DataReader) PeekULong() uint64  { return binary.LittleEndian.Uint64(r.peek(8)) }
func (r *DataReader) PeekLong() int64    { return int64(r.PeekULong()) }
func (r *DataReader) PeekFloat() float32 { return math.Float32frombits(r.PeekUInt()) }
func (r *DataReader) PeekDouble() float64 {
	return math.Float64frombits(r.PeekULong())
}

// PeekString is GetString without moving the position.
func (r *DataReader) PeekString() string {
	pos := r.position
	defer func() { r.position = pos }()
	return r.GetString()
}

// PeekStringMax is GetStringMax without moving the position.
func (r *DataReader) PeekStringMax(maxLength int) string {
	pos := r.position
	defer func() { r.position = pos }()
	return r.GetStringMax(maxLength)
}

func tryGet[T any](r *DataReader, size int, get func() T) (T, bool) {
	if r.AvailableBytes() >= size {
		return get(), true
	}
	var zero T
	return zero, false
}

func (r *DataReader) TryGetByte() (byte, bool)      { return tryGet(r, 1, r.GetByte) }
func (r *DataReader) TryGetSByte() (int8, bool)     { return tryGet(r, 1, r.GetSByte) }
func (r *DataReader) TryGetBool() (bool, bool)      { return tryGet(r, 1, r.GetBool) }
func (r *DataReader) TryGetChar() (uint16, bool)    { return tryGet(r, 2, r.GetChar) }
func (r *DataReader) TryGetShort() (int16, bool)    { return tryGet(r, 2, r.GetShort) }
func (r *DataReader) TryGetUShort() (uint16, bool)  { return tryGet(r, 2, r.GetUShort) }
func (r *DataReader) TryGetInt() (int32, bool)      { return tryGet(r, 4, r.GetInt) }
func (r *DataReader) TryGetUInt() (uint32, bool)    { return tryGet(r, 4, r.GetUInt) }
func (r *DataReader) TryGetLong() (int64, bool)     { return tryGet(r, 8, r.GetLong) }
func (r *DataReader) TryGetULong() (uint64, bool)   { return tryGet(r, 8, r.GetULong) }
func (r *DataReader) TryGetFloat() (float32, bool)  { return tryGet(r, 4, r.GetFloat) }
func (r *DataReader) TryGetDouble() (float64, bool) { return tryGet(r, 8, r.GetDouble) }

// TryGetString reads a string if it is complete.
func (r *DataReader) TryGetString() (string, bool) {
	if r.AvailableBytes() >= 4 && r.AvailableBytes() >= int(r.PeekInt())+4 {
		return r.GetString(), true
	}
	return "", false
}

// TryGetStringArray reads a string array if it is complete.
func (r *DataReader) TryGetStringArray() ([]string, bool) {
	size, ok := r.TryGetUShort()
	if !ok {
		return nil, false
	}
	arr := make([]string, size)
	for i := range arr {
		if arr[i], ok = r.TryGetString(); !ok {
			return nil, false
		}
	}
	return arr, true
}

// TryGetBytesWithLength reads a length-prefixed byte slice if it is complete.
func (r *DataReader) TryGetBytesWithLength() ([]byte, bool) {
	if r.AvailableBytes() >= 4 && r.AvailableBytes() >= int(r.PeekInt())+4 {
		return r.GetBytesWithLength(), true
	}
	return nil, false
}

// utf16Len counts UTF-16 code units the way .NET decodes s.
func utf16Len(s string) int {
	n := 0
	for _, c := range s {
		if c > 0xFFFF {
			n += 2
		} else {
			n++
		}
	}
	return n
}
