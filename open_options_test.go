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
