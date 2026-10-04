package engine

import (
	"context"
	"io"
)

const backupIOChunk = 64 << 10

type backupContextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (reader *backupContextReader) Read(data []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	return reader.reader.Read(data[:min(len(data), backupIOChunk)])
}

type backupContextWriter struct {
	ctx    context.Context
	writer io.Writer
}

func (writer backupContextWriter) Write(data []byte) (int, error) {
	written := 0
	for len(data) != 0 {
		if err := writer.ctx.Err(); err != nil {
			return written, err
		}
		chunk := data[:min(len(data), backupIOChunk)]
		n, err := writer.writer.Write(chunk)
		written += n
		if err != nil {
			return written, err
		}
		if n != len(chunk) {
			return written, io.ErrShortWrite
		}
		data = data[n:]
	}
	return written, nil
}
