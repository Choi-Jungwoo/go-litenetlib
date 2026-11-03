package litenetlib

import (
	"errors"
	"fmt"
	"reflect"
	"unicode/utf16"
)

// Serializable is a type that writes and reads itself (INetSerializable).
type Serializable interface {
	Serialize(w *DataWriter)
	Deserialize(r *DataReader)
}

var (
	// ErrUndefinedPacket is returned by ReadPacket for a hash with no subscription.
	ErrUndefinedPacket = errors.New("litenetlib: undefined packet in DataReader")
	// ErrMalformedPacket is returned by ReadPacket for a packet that ends early; it is not delivered.
	ErrMalformedPacket = errors.New("litenetlib: malformed packet in DataReader")
)

// PacketProcessor routes packets prefixed with a type hash to callbacks, serializing
// struct fields like NetPacketProcessor/NetSerializer. A packet type is identified by the
// full name of its C# counterpart (e.g. "MyGame.Packets.JoinPacket"); exported fields map to
// its public properties in declaration order. It is not safe for concurrent use.
type PacketProcessor struct {
	maxStringLength int
	callbacks       map[uint64]func(r *DataReader, userData any) error
	nested          map[reflect.Type]codec
	structs         map[reflect.Type][]fieldCodec
	writer          DataWriter // reused by the Send helpers
}

type codec struct {
	write func(w *DataWriter, v reflect.Value)
	read  func(r *DataReader, v reflect.Value)
}

type fieldCodec struct {
	index int
	codec
}

// NewPacketProcessor returns a processor; maxStringLength > 0 limits strings to that many UTF-16 units.
func NewPacketProcessor(maxStringLength int) *PacketProcessor {
	return &PacketProcessor{
		maxStringLength: maxStringLength,
		callbacks:       make(map[uint64]func(*DataReader, any) error),
		nested:          make(map[reflect.Type]codec),
		structs:         make(map[reflect.Type][]fieldCodec),
	}
}

// PacketHash is the 64-bit FNV hash of a C# type full name, as NetPacketProcessor.GetHash.
func PacketHash(typeName string) uint64 {
	hash := uint64(14695981039346656037)
	for _, c := range utf16.Encode([]rune(typeName)) {
		hash ^= uint64(c)
		hash *= 1099511628211
	}
	return hash
}

// RegisterNestedType registers custom serialization for fields of type T. It reports false if T
// already has a codec or is a primitive (NetSerializer.BasicTypes). Codecs for slices of
// primitives are accepted but, as in the reference, the built-in array format wins.
func RegisterNestedType[T any](pp *PacketProcessor, write func(w *DataWriter, v T), read func(r *DataReader) T) bool {
	t := reflect.TypeFor[T]()
	if _, ok := pp.nested[t]; ok || primitive(t) {
		return false
	}
	pp.nested[t] = codec{
		write: func(w *DataWriter, v reflect.Value) { write(w, v.Interface().(T)) },
		read:  func(r *DataReader, v reflect.Value) { v.Set(reflect.ValueOf(read(r))) },
	}
	return true
}

// Subscribe calls onReceive with a new *T for every packet named typeName.
func Subscribe[T any](pp *PacketProcessor, typeName string, onReceive func(packet *T, userData any)) error {
	return subscribe(pp, typeName, onReceive, func() *T { return new(T) })
}

// SubscribeReusable is Subscribe reusing one *T for all packets.
func SubscribeReusable[T any](pp *PacketProcessor, typeName string, onReceive func(packet *T, userData any)) error {
	p := new(T)
	return subscribe(pp, typeName, onReceive, func() *T { return p })
}

func subscribe[T any](pp *PacketProcessor, typeName string, onReceive func(*T, any), packet func() *T) error {
	fields, err := pp.register(reflect.TypeFor[T]())
	if err != nil {
		return err
	}
	pp.callbacks[PacketHash(typeName)] = func(r *DataReader, userData any) error {
		p := packet()
		if !readFields(r, reflect.ValueOf(p).Elem(), fields) {
			return ErrMalformedPacket
		}
		onReceive(p, userData)
		return nil
	}
	return nil
}

// SubscribeNetSerializable calls onReceive with a new deserialized PT for every packet named typeName.
func SubscribeNetSerializable[T any, PT interface {
	*T
	Serializable
}](pp *PacketProcessor, typeName string, onReceive func(packet PT, userData any)) {
	pp.callbacks[PacketHash(typeName)] = func(r *DataReader, userData any) error {
		p := PT(new(T))
		if !deserialize(p, r) {
			return ErrMalformedPacket
		}
		onReceive(p, userData)
		return nil
	}
}

// RemoveSubscription removes the callback for typeName.
func (pp *PacketProcessor) RemoveSubscription(typeName string) bool {
	h := PacketHash(typeName)
	_, ok := pp.callbacks[h]
	delete(pp.callbacks, h)
	return ok
}

// Write writes the hash of typeName followed by the fields of packet.
func Write[T any](pp *PacketProcessor, w *DataWriter, typeName string, packet *T) error {
	fields, err := pp.register(reflect.TypeFor[T]())
	if err != nil {
		return err
	}
	w.PutULong(PacketHash(typeName))
	v := reflect.ValueOf(packet).Elem()
	for _, f := range fields {
		f.write(w, v.Field(f.index))
	}
	return nil
}

// WriteNetSerializable writes the hash of typeName followed by packet.
func (pp *PacketProcessor) WriteNetSerializable(w *DataWriter, typeName string, packet Serializable) {
	w.PutULong(PacketHash(typeName))
	packet.Serialize(w)
}

// ReadPacket reads one packet and calls its subscription.
func (pp *PacketProcessor) ReadPacket(r *DataReader, userData any) error {
	if r.AvailableBytes() < 8 { // the uint64 type hash
		return ErrMalformedPacket
	}
	cb, ok := pp.callbacks[r.GetULong()]
	if !ok {
		return ErrUndefinedPacket
	}
	return cb(r, userData)
}

// ReadAllPackets reads packets until the reader is exhausted.
func (pp *PacketProcessor) ReadAllPackets(r *DataReader, userData any) error {
	for r.AvailableBytes() > 0 {
		if err := pp.ReadPacket(r, userData); err != nil {
			return err
		}
	}
	return nil
}

// readFields and deserialize report whether the packet was read completely; reading past
// the end panics. The reference still delivers a partially read packet; this port drops it.
func readFields(r *DataReader, v reflect.Value, fields []fieldCodec) (ok bool) {
	defer func() { _ = recover() }() // a panic leaves ok false
	for _, f := range fields {
		f.read(r, v.Field(f.index))
	}
	return true
}

func deserialize(p Serializable, r *DataReader) (ok bool) {
	defer func() { _ = recover() }() // a panic leaves ok false
	p.Deserialize(r)
	return true
}

func (pp *PacketProcessor) register(t reflect.Type) ([]fieldCodec, error) {
	if fields, ok := pp.structs[t]; ok {
		return fields, nil
	}
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("litenetlib: packet type %v is not a struct", t)
	}
	var fields []fieldCodec
	for i := range t.NumField() {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		c, err := pp.codecFor(f.Type)
		if err != nil {
			return nil, fmt.Errorf("litenetlib: field %v.%s: %w", t, f.Name, err)
		}
		fields = append(fields, fieldCodec{i, c})
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("litenetlib: type %v does not contain acceptable fields", t)
	}
	pp.structs[t] = fields
	return fields, nil
}

var serializableType = reflect.TypeFor[Serializable]()

// primitive reports whether t is a predeclared type that maps to a C# primitive. NetSerializer
// handles primitives and their arrays before registered nested types; named types map to C#
// enums and structs.
func primitive(t reflect.Type) bool {
	if t.Name() == "" || t.PkgPath() != "" {
		return false
	}
	switch t.Kind() {
	case reflect.Bool, reflect.Uint8, reflect.Int8, reflect.Int16, reflect.Uint16, reflect.Int32,
		reflect.Uint32, reflect.Int64, reflect.Uint64, reflect.Float32, reflect.Float64, reflect.String:
		return true
	}
	return false
}

func (pp *PacketProcessor) codecFor(t reflect.Type) (codec, error) {
	builtin := primitive(t) || t.Kind() == reflect.Slice && t.Name() == "" && primitive(t.Elem())
	if c, ok := pp.nested[t]; ok && !builtin {
		return c, nil
	}
	if reflect.PointerTo(t).Implements(serializableType) {
		return codec{
			write: func(w *DataWriter, v reflect.Value) { v.Addr().Interface().(Serializable).Serialize(w) },
			read:  func(r *DataReader, v reflect.Value) { v.Addr().Interface().(Serializable).Deserialize(r) },
		}, nil
	}
	switch t.Kind() {
	case reflect.Bool:
		return codec{
			func(w *DataWriter, v reflect.Value) { w.PutBool(v.Bool()) },
			func(r *DataReader, v reflect.Value) { v.SetBool(r.GetBool()) }}, nil
	case reflect.Uint8:
		return codec{
			func(w *DataWriter, v reflect.Value) { w.PutByte(uint8(v.Uint())) },
			func(r *DataReader, v reflect.Value) { v.SetUint(uint64(r.GetByte())) }}, nil
	case reflect.Int8:
		return codec{
			func(w *DataWriter, v reflect.Value) { w.PutSByte(int8(v.Int())) },
			func(r *DataReader, v reflect.Value) { v.SetInt(int64(r.GetSByte())) }}, nil
	case reflect.Int16:
		return codec{
			func(w *DataWriter, v reflect.Value) { w.PutShort(int16(v.Int())) },
			func(r *DataReader, v reflect.Value) { v.SetInt(int64(r.GetShort())) }}, nil
	case reflect.Uint16: // ushort or char
		return codec{
			func(w *DataWriter, v reflect.Value) { w.PutUShort(uint16(v.Uint())) },
			func(r *DataReader, v reflect.Value) { v.SetUint(uint64(r.GetUShort())) }}, nil
	case reflect.Int32:
		return codec{
			func(w *DataWriter, v reflect.Value) { w.PutInt(int32(v.Int())) },
			func(r *DataReader, v reflect.Value) { v.SetInt(int64(r.GetInt())) }}, nil
	case reflect.Uint32:
		return codec{
			func(w *DataWriter, v reflect.Value) { w.PutUInt(uint32(v.Uint())) },
			func(r *DataReader, v reflect.Value) { v.SetUint(uint64(r.GetUInt())) }}, nil
	case reflect.Int64:
		return codec{
			func(w *DataWriter, v reflect.Value) { w.PutLong(v.Int()) },
			func(r *DataReader, v reflect.Value) { v.SetInt(r.GetLong()) }}, nil
	case reflect.Uint64:
		return codec{
			func(w *DataWriter, v reflect.Value) { w.PutULong(v.Uint()) },
			func(r *DataReader, v reflect.Value) { v.SetUint(r.GetULong()) }}, nil
	case reflect.Float32:
		return codec{
			func(w *DataWriter, v reflect.Value) { w.PutFloat(float32(v.Float())) },
			func(r *DataReader, v reflect.Value) { v.SetFloat(float64(r.GetFloat())) }}, nil
	case reflect.Float64:
		return codec{
			func(w *DataWriter, v reflect.Value) { w.PutDouble(v.Float()) },
			func(r *DataReader, v reflect.Value) { v.SetFloat(r.GetDouble()) }}, nil
	case reflect.String:
		if n := pp.maxStringLength; n > 0 {
			return codec{
				func(w *DataWriter, v reflect.Value) { w.PutStringMax(v.String(), n) },
				func(r *DataReader, v reflect.Value) { v.SetString(r.GetStringMax(n)) }}, nil
		}
		return codec{
			func(w *DataWriter, v reflect.Value) { w.PutString(v.String()) },
			func(r *DataReader, v reflect.Value) { v.SetString(r.GetString()) }}, nil
	case reflect.Slice:
		if t.Elem() == reflect.TypeFor[byte]() { // byte[] is length-prefixed with an int32
			return codec{
				func(w *DataWriter, v reflect.Value) { w.PutBytesWithLength(v.Bytes()) },
				func(r *DataReader, v reflect.Value) { v.SetBytes(r.GetBytesWithLength()) }}, nil
		}
		elem, err := pp.codecFor(t.Elem())
		if err != nil {
			return codec{}, err
		}
		return codec{
			func(w *DataWriter, v reflect.Value) {
				n := uint16(v.Len())
				w.PutUShort(n)
				for i := range int(n) {
					elem.write(w, v.Index(i))
				}
			},
			func(r *DataReader, v reflect.Value) {
				n := int(r.GetUShort())
				s := reflect.MakeSlice(t, n, n)
				for i := range s.Len() {
					elem.read(r, s.Index(i))
				}
				v.Set(s)
			}}, nil
	}
	return codec{}, fmt.Errorf("unsupported type %v", t)
}

// Send writes packet like Write and sends it to peer.
func Send[T any](pp *PacketProcessor, peer *Peer, typeName string, packet *T, method DeliveryMethod) error {
	pp.writer.Reset()
	if err := Write(pp, &pp.writer, typeName, packet); err != nil {
		return err
	}
	return peer.Send(pp.writer.Data(), method)
}

// SendToAll writes packet like Write and sends it to all peers of m.
func SendToAll[T any](pp *PacketProcessor, m *Manager, typeName string, packet *T, method DeliveryMethod) error {
	pp.writer.Reset()
	if err := Write(pp, &pp.writer, typeName, packet); err != nil {
		return err
	}
	return m.SendToAll(pp.writer.Data(), method)
}

// SendNetSerializable writes packet like WriteNetSerializable and sends it to peer.
func (pp *PacketProcessor) SendNetSerializable(peer *Peer, typeName string, packet Serializable, method DeliveryMethod) error {
	pp.writer.Reset()
	pp.WriteNetSerializable(&pp.writer, typeName, packet)
	return peer.Send(pp.writer.Data(), method)
}

// SendNetSerializableToAll writes packet like WriteNetSerializable and sends it to all peers of m.
func (pp *PacketProcessor) SendNetSerializableToAll(m *Manager, typeName string, packet Serializable, method DeliveryMethod) error {
	pp.writer.Reset()
	pp.WriteNetSerializable(&pp.writer, typeName, packet)
	return m.SendToAll(pp.writer.Data(), method)
}
