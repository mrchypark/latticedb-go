package store

import (
	"strconv"
	"testing"
)

func BenchmarkStringPostingsUniqueKeys10K(b *testing.B) {
	for range b.N {
		postings := NewStringPostings()
		for index := range 10_000 {
			postings.Add(strconv.Itoa(index), uint64(index+1))
		}
		if postings.Len("9999") != 1 {
			b.Fatal("missing posting")
		}
	}
}
