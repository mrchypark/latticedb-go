package latticedb

import (
	"errors"
	"strings"
	"testing"
)

func TestOpenOptionsUnsupportedRequests(t *testing.T) {
	tests := []struct {
		name string
		opts OpenOptions
		want string
	}{
		{name: "disable WAL", opts: OpenOptions{DisableWAL: true}, want: "disabling WAL"},
		{name: "enable WAL", opts: OpenOptions{EnableWAL: true}, want: "EnableWAL"},
		{name: "adjacency cache", opts: OpenOptions{EnableAdjacencyCache: true}, want: "EnableAdjacencyCache"},
		{name: "legacy cache size", opts: OpenOptions{CacheSizeMB: 100}, want: "CacheSizeMB"},
		{name: "cache size", opts: OpenOptions{CacheSizeMB: 1}, want: "CacheSizeMB"},
		{name: "legacy page size", opts: OpenOptions{PageSize: 4096}, want: "PageSize"},
		{name: "page size", opts: OpenOptions{PageSize: 1}, want: "PageSize"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, open := range []func() error{
				func() error { _, err := Open(t.TempDir(), test.opts); return err },
				func() error { _, err := Deserialize(nil, test.opts); return err },
			} {
				err := open()
				var latticeErr *Error
				if !errors.As(err, &latticeErr) || !errors.Is(err, ErrUnsupportedOption) || !strings.Contains(err.Error(), test.want) {
					t.Fatalf("error = %v", err)
				}
			}
		})
	}
}

func TestOpenOptionsDefaultsRemainCompatible(t *testing.T) {
	db, err := Open(t.TempDir(), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestVectorZeroDimensionAndDisabledCompatibility(t *testing.T) {
	db, err := Open(t.TempDir(), OpenOptions{Create: true, EnableVector: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	vector := make([]float32, 128)
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]Value{"embedding": vector}})
		return err
	}); err != nil {
		t.Fatalf("zero dimensions did not use public default: %v", err)
	}

	disabled, err := Open(t.TempDir(), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer disabled.Close()
	if err := disabled.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]Value{
			"a": []float32{1}, "b": []float32{2, 3},
		}})
		return err
	}); err != nil {
		t.Fatalf("disabled database rejected arbitrary vectors: %v", err)
	}
}
