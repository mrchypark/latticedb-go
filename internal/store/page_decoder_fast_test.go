package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"testing"
)

func pageDecoderTestRecord(kind byte, body []byte) []byte {
	record := append([]byte{pageRecordVersion, kind, 0, 0, 0, 0}, body...)
	binary.BigEndian.PutUint32(record[2:6], crc32.ChecksumIEEE(body))
	return record
}

func pageDecoderTestPair(t *testing.T, body []byte, maxBytes uint64) (*binaryDecoder, *binaryDecoder) {
	t.Helper()
	fast, err := decodePageRecord(pageDecoderTestRecord(99, body), 99, maxBytes)
	if err != nil {
		t.Fatal(err)
	}
	return fast, newBinaryDecoder(bytes.NewReader(body), uint64(len(body)), maxBytes)
}

func pageDecoderTestError(t *testing.T, fast, stream *binaryDecoder) {
	t.Helper()
	if got, want := fast.errString(), stream.errString(); got != want {
		t.Fatalf("error = %q, want %q", got, want)
	}
	if errors.Is(fast.err, io.ErrUnexpectedEOF) != errors.Is(stream.err, io.ErrUnexpectedEOF) {
		t.Fatalf("unexpected EOF fast=%v stream=%v", fast.err, stream.err)
	}
	if fast.remaining != stream.remaining {
		t.Fatalf("remaining fast=%d stream=%d", fast.remaining, stream.remaining)
	}
	if got, want := finishError(fast), finishError(stream); got != want {
		t.Fatalf("finish error = %q, want %q", got, want)
	}
}

func (d *binaryDecoder) errString() string {
	if d.err == nil {
		return ""
	}
	return d.err.Error()
}

func finishError(d *binaryDecoder) string {
	if err := d.finish(); err != nil {
		return err.Error()
	}
	return ""
}

func TestPageDecoderFastMatchesStream(t *testing.T) {
	var body bytes.Buffer
	body.WriteByte(0xa5)
	var scratch [binary.MaxVarintLen64]byte
	body.Write(scratch[:binary.PutUvarint(scratch[:], ^uint64(0))])
	body.Write(scratch[:binary.PutVarint(scratch[:], -42)])
	body.Write([]byte{3, 4})
	body.WriteByte(1)
	fast, stream := pageDecoderTestPair(t, body.Bytes(), 64)
	for _, d := range []*binaryDecoder{fast, stream} {
		if got := mustReadByte(t, d); got != 0xa5 {
			t.Fatalf("byte = %x", got)
		}
		if got := d.u(); got != ^uint64(0) {
			t.Fatalf("uvarint = %d", got)
		}
		if got := d.i(); got != -42 {
			t.Fatalf("varint = %d", got)
		}
		var raw [2]byte
		d.read(raw[:])
		if raw != [2]byte{3, 4} {
			t.Fatalf("raw = %v", raw)
		}
		if got := d.u(); got != 1 {
			t.Fatalf("tail uvarint = %d", got)
		}
	}
	pageDecoderTestError(t, fast, stream)
}

func TestPageDecoderFastMatchesStreamMalformedVarints(t *testing.T) {
	cases := []struct {
		name string
		body []byte
		read func(*binaryDecoder)
	}{
		{"truncated unsigned", []byte{0x80}, func(d *binaryDecoder) { d.u() }},
		{"truncated signed", []byte{0x80}, func(d *binaryDecoder) { d.i() }},
		{"overflow unsigned", bytes.Repeat([]byte{0x80}, binary.MaxVarintLen64), func(d *binaryDecoder) { d.u() }},
		{"overflow signed", append(bytes.Repeat([]byte{0x80}, binary.MaxVarintLen64-1), 2), func(d *binaryDecoder) { d.i() }},
		{"overflow after ten continuation bytes", append(bytes.Repeat([]byte{0x80}, binary.MaxVarintLen64), 1), func(d *binaryDecoder) { d.u() }},
		{"overflow after eleven continuation bytes", append(bytes.Repeat([]byte{0x80}, binary.MaxVarintLen64+1), 1), func(d *binaryDecoder) { d.u() }},
		{"overflow after twelve continuation bytes", append(bytes.Repeat([]byte{0x80}, binary.MaxVarintLen64+2), 1), func(d *binaryDecoder) { d.u() }},
		{"exhausted bounds", nil, func(d *binaryDecoder) { _, _ = d.ReadByte() }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fast, stream := pageDecoderTestPair(t, tc.body, 64)
			tc.read(fast)
			tc.read(stream)
			pageDecoderTestError(t, fast, stream)
		})
	}
}

func TestPageDecoderFastPreservesLimitsAndTrailingData(t *testing.T) {
	body := []byte{binaryMapValue, 2, 1, 'a', binaryEmptyValue}
	fast, stream := pageDecoderTestPair(t, body, uint64(len(body)+6))
	fast.value(0)
	stream.value(0)
	pageDecoderTestError(t, fast, stream)

	fast, stream = pageDecoderTestPair(t, []byte{1, 2}, 64)
	if fast.u() != 1 || stream.u() != 1 {
		t.Fatal("first value did not decode")
	}
	pageDecoderTestError(t, fast, stream)
}

func mustReadByte(t *testing.T, d *binaryDecoder) byte {
	t.Helper()
	b, err := d.ReadByte()
	if err != nil {
		t.Fatal(err)
	}
	return b
}
