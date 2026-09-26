package pagestore

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
)

// IsFile recognizes either bbolt meta page; Open subsequently validates the
// version, page size, and checksum. A damaged first meta page can be recovered
// from the second one without misrouting the file as a legacy checkpoint.
func IsFile(path string) (bool, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer file.Close()
	var header [24]byte
	offsets := []int64{0, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536}
	for _, offset := range offsets {
		_, err := file.ReadAt(header[:], offset)
		if errors.Is(err, io.EOF) {
			continue
		}
		if err != nil {
			return false, err
		}
		if (binary.LittleEndian.Uint32(header[16:20]) == 0xED0CDAED && binary.LittleEndian.Uint16(header[8:10]) == 4) || (binary.BigEndian.Uint32(header[16:20]) == 0xED0CDAED && binary.BigEndian.Uint16(header[8:10]) == 4) {
			return true, nil
		}
	}
	return false, nil
}
