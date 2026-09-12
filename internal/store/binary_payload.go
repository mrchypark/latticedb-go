package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"unicode/utf8"
)

// Changefeed envelopes add one level around a property at the public depth limit.
const maxBinaryValueDepth = maxValueDepth + 1

// Wire tags belong to state v5 and WAL v4; their numeric values must not change.
const (
	binaryEmptyValue byte = iota
	binaryNullValue
	binaryBoolValue
	binaryIntValue
	binaryFloatValue
	binaryStringValue
	binaryBytesValue
	binaryVectorValue
	binaryListValue
	binaryMapValue
)

type binaryEncoder struct {
	out     io.Writer
	scratch [10]byte
	err     error
}

func (e *binaryEncoder) write(data []byte) {
	if e.err != nil {
		return
	}
	var n int
	n, e.err = e.out.Write(data)
	if e.err == nil && n != len(data) {
		e.err = io.ErrShortWrite
	}
}
func (e *binaryEncoder) u(value uint64) {
	n := binary.PutUvarint(e.scratch[:], value)
	e.write(e.scratch[:n])
}
func (e *binaryEncoder) i(value int64) {
	n := binary.PutVarint(e.scratch[:], value)
	e.write(e.scratch[:n])
}
func (e *binaryEncoder) tag(value byte) { e.scratch[0] = value; e.write(e.scratch[:1]) }
func (e *binaryEncoder) str(value string) {
	e.u(uint64(len(value)))
	if e.err != nil {
		return
	}
	n, err := io.WriteString(e.out, value)
	e.err = err
	if err == nil && n != len(value) {
		e.err = io.ErrShortWrite
	}
}
func (e *binaryEncoder) bytes(value []byte) {
	if value == nil {
		e.u(0)
		return
	}
	e.u(uint64(len(value)) + 1)
	e.write(value)
}
func (e *binaryEncoder) strings(values []string) {
	if e.err != nil {
		return
	}
	if values == nil {
		e.u(0)
		return
	}
	e.u(uint64(len(values)) + 1)
	for _, value := range values {
		e.str(value)
		if e.err != nil {
			return
		}
	}
}
func (e *binaryEncoder) ids(values []uint64) {
	if e.err != nil {
		return
	}
	if values == nil {
		e.u(0)
		return
	}
	e.u(uint64(len(values)) + 1)
	for _, value := range values {
		e.u(value)
		if e.err != nil {
			return
		}
	}
}
func (e *binaryEncoder) properties(values map[string]persistedValue, depth int) {
	if e.err != nil {
		return
	}
	if depth > maxBinaryValueDepth {
		e.err = fmt.Errorf("%w: property nesting", ErrValueLimit)
		return
	}
	if values == nil {
		e.u(0)
		return
	}
	e.u(uint64(len(values)) + 1)
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		e.str(key)
		e.value(values[key], depth)
		if e.err != nil {
			return
		}
	}
}
func (e *binaryEncoder) value(value persistedValue, depth int) {
	if e.err != nil {
		return
	}
	if depth > maxBinaryValueDepth {
		e.err = fmt.Errorf("%w: property nesting", ErrValueLimit)
		return
	}
	switch value.Kind {
	case "":
		e.tag(binaryEmptyValue)
	case "null":
		e.tag(binaryNullValue)
	case "bool":
		e.tag(binaryBoolValue)
		if value.Bool {
			e.tag(1)
		} else {
			e.tag(0)
		}
	case "int":
		e.tag(binaryIntValue)
		e.i(value.Int)
	case "float":
		if math.IsNaN(value.Float) || math.IsInf(value.Float, 0) {
			e.err = errors.New("non-finite binary property")
			return
		}
		e.tag(binaryFloatValue)
		binary.BigEndian.PutUint64(e.scratch[:8], math.Float64bits(value.Float))
		e.write(e.scratch[:8])
	case "string":
		e.tag(binaryStringValue)
		e.str(value.String)
	case "bytes":
		e.tag(binaryBytesValue)
		e.bytes(value.Bytes)
	case "vector":
		e.tag(binaryVectorValue)
		if value.Vector == nil {
			e.u(0)
			return
		}
		e.u(uint64(len(value.Vector)) + 1)
		for _, item := range value.Vector {
			if math.IsNaN(float64(item)) || math.IsInf(float64(item), 0) {
				e.err = errors.New("non-finite binary vector")
				return
			}
			binary.BigEndian.PutUint32(e.scratch[:4], math.Float32bits(item))
			e.write(e.scratch[:4])
			if e.err != nil {
				return
			}
		}
	case "list":
		e.tag(binaryListValue)
		if value.List == nil {
			e.u(0)
			return
		}
		e.u(uint64(len(value.List)) + 1)
		for _, item := range value.List {
			e.value(item, depth+1)
			if e.err != nil {
				return
			}
		}
	case "map":
		e.tag(binaryMapValue)
		e.properties(value.Map, depth+1)
	default:
		e.err = fmt.Errorf("unsupported binary property kind %q", value.Kind)
	}
}

type binaryDecoder struct {
	in             io.Reader
	remaining      uint64
	allocationLeft uint64
	scratch        [8]byte
	err            error
}

func newBinaryDecoder(in io.Reader, length, maxBytes uint64) *binaryDecoder {
	return &binaryDecoder{in: in, remaining: length, allocationLeft: multiplySaturated(maxBytes, 2)}
}
func (d *binaryDecoder) read(data []byte) {
	if d.err != nil {
		return
	}
	if uint64(len(data)) > d.remaining {
		d.err = io.ErrUnexpectedEOF
		return
	}
	_, d.err = io.ReadFull(d.in, data)
	d.remaining -= uint64(len(data))
}
func (d *binaryDecoder) ReadByte() (byte, error) { d.read(d.scratch[:1]); return d.scratch[0], d.err }
func (d *binaryDecoder) u() uint64 {
	if d.err != nil {
		return 0
	}
	value, err := binary.ReadUvarint(d)
	if err != nil {
		d.err = err
		return 0
	}
	return value
}
func (d *binaryDecoder) i() int64 {
	if d.err != nil {
		return 0
	}
	value, err := binary.ReadVarint(d)
	if err != nil {
		d.err = err
		return 0
	}
	return value
}
func (d *binaryDecoder) reserve(count, size uint64) bool {
	if d.err != nil {
		return false
	}
	if size != 0 && count > d.allocationLeft/size {
		d.err = fmt.Errorf("%w: binary decoded allocation exceeds limit", ErrLoadResourceLimit)
		return false
	}
	d.allocationLeft -= count * size
	return true
}

// A present collection has length+1; zero preserves a nil collection.
// Each element must occupy at least one remaining wire byte.
func (d *binaryDecoder) length(elementBytes uint64) int {
	encoded := d.u()
	if d.err != nil || encoded == 0 {
		return -1
	}
	count := encoded - 1
	if count > d.remaining || count > uint64(^uint(0)>>1) {
		d.err = fmt.Errorf("%w: invalid binary collection length", ErrLoadResourceLimit)
		return -1
	}
	if !d.reserve(count, elementBytes) {
		return -1
	}
	return int(count)
}
func (d *binaryDecoder) str() string {
	size := d.u()
	if d.err != nil {
		return ""
	}
	if size > d.remaining || size > uint64(^uint(0)>>1) {
		d.err = fmt.Errorf("%w: invalid binary string length", ErrLoadResourceLimit)
		return ""
	}
	// Include the temporary byte buffer and the owned string.
	if !d.reserve(size, 2) {
		return ""
	}
	data := make([]byte, int(size))
	d.read(data)
	if d.err == nil && !utf8.Valid(data) {
		d.err = errors.New("invalid UTF-8 in binary string")
	}
	return string(data)
}
func (d *binaryDecoder) bytes() []byte {
	n := d.length(1)
	if n < 0 {
		return nil
	}
	values := make([]byte, n)
	d.read(values)
	return values
}
func (d *binaryDecoder) strings() []string {
	n := d.length(16)
	if n < 0 {
		return nil
	}
	values := make([]string, n)
	for i := range values {
		values[i] = d.str()
		if d.err != nil {
			break
		}
	}
	return values
}
func (d *binaryDecoder) ids() []uint64 {
	n := d.length(8)
	if n < 0 {
		return nil
	}
	values := make([]uint64, n)
	for i := range values {
		values[i] = d.u()
		if d.err != nil {
			break
		}
	}
	return values
}
func (d *binaryDecoder) properties(depth int) map[string]persistedValue {
	if depth > maxBinaryValueDepth {
		d.err = fmt.Errorf("%w: property nesting", ErrValueLimit)
		return nil
	}
	n := d.length(208)
	if n < 0 {
		return nil
	}
	values := make(map[string]persistedValue, n)
	for range n {
		key := d.str()
		value := d.value(depth)
		if d.err != nil {
			break
		}
		if _, exists := values[key]; exists {
			d.err = errors.New("duplicate binary property key")
			break
		}
		values[key] = value
	}
	return values
}
func (d *binaryDecoder) value(depth int) persistedValue {
	var value persistedValue
	if d.err != nil {
		return value
	}
	if depth > maxBinaryValueDepth {
		d.err = fmt.Errorf("%w: property nesting", ErrValueLimit)
		return value
	}
	tag, err := d.ReadByte()
	if err != nil {
		return value
	}
	switch tag {
	case binaryEmptyValue:
	case binaryNullValue:
		value.Kind = "null"
	case binaryBoolValue:
		value.Kind = "bool"
		flag, err := d.ReadByte()
		if err == nil && flag > 1 {
			d.err = errors.New("invalid binary boolean")
		}
		value.Bool = flag == 1
	case binaryIntValue:
		value.Kind = "int"
		value.Int = d.i()
	case binaryFloatValue:
		value.Kind = "float"
		d.read(d.scratch[:8])
		value.Float = math.Float64frombits(binary.BigEndian.Uint64(d.scratch[:8]))
		if d.err == nil && (math.IsNaN(value.Float) || math.IsInf(value.Float, 0)) {
			d.err = errors.New("non-finite binary property")
		}
	case binaryStringValue:
		value.Kind = "string"
		value.String = d.str()
	case binaryBytesValue:
		value.Kind = "bytes"
		value.Bytes = d.bytes()
	case binaryVectorValue:
		value.Kind = "vector"
		n := d.length(4)
		if n < 0 {
			return value
		}
		if uint64(n) > d.remaining/4 {
			d.err = io.ErrUnexpectedEOF
			return value
		}
		value.Vector = make([]float32, n)
		for i := range value.Vector {
			d.read(d.scratch[:4])
			value.Vector[i] = math.Float32frombits(binary.BigEndian.Uint32(d.scratch[:4]))
			if d.err != nil {
				break
			}
			if math.IsNaN(float64(value.Vector[i])) || math.IsInf(float64(value.Vector[i]), 0) {
				d.err = errors.New("non-finite binary vector")
				break
			}
		}
	case binaryListValue:
		value.Kind = "list"
		n := d.length(136)
		if n < 0 {
			return value
		}
		value.List = make([]persistedValue, n)
		for i := range value.List {
			value.List[i] = d.value(depth + 1)
			if d.err != nil {
				break
			}
		}
	case binaryMapValue:
		value.Kind = "map"
		value.Map = d.properties(depth + 1)
	default:
		d.err = fmt.Errorf("unknown binary property tag %d", tag)
	}
	return value
}
func (d *binaryDecoder) finish() error {
	if d.err != nil {
		return d.err
	}
	if d.remaining != 0 {
		return errors.New("binary payload has trailing data")
	}
	return nil
}

func encodeBinarySlice[T any](e *binaryEncoder, values []T, write func(T)) {
	if e.err != nil {
		return
	}
	if values == nil {
		e.u(0)
		return
	}
	e.u(uint64(len(values)) + 1)
	for _, value := range values {
		write(value)
		if e.err != nil {
			return
		}
	}
}

func decodeBinarySlice[T any](d *binaryDecoder, elementBytes uint64, read func() T) []T {
	n := d.length(elementBytes)
	if n < 0 {
		return nil
	}
	values := make([]T, n)
	for i := range values {
		values[i] = read()
		if d.err != nil {
			break
		}
	}
	return values
}
