package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func BenchmarkVectorCacheColdOpen(b *testing.B) {
	for _, size := range []int{1_000, 10_000} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			dir := b.TempDir()
			path := filepath.Join(dir, "bench")
			// One Create + one Update seeds all nodes deterministically.
			db, err := Open(path, OpenOptions{
				Create:           true,
				EnableVector:     true,
				VectorDimensions: 16,
				VectorIndexMode:  VectorIndexHNSWSynchronous,
			})
			if err != nil {
				b.Fatal(err)
			}
			if err := db.Update(func(tx *Tx) error {
				for id := uint64(1); id <= uint64(size); id++ {
					vec := deterministicVector(id, 16)
					if _, err := tx.CreateNode(CreateNodeOptions{
						Properties: map[string]any{"v": vec},
					}); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				_ = db.Close()
				b.Fatal(err)
			}
			sidecar := db.files.State + "-hnsw"
			if err := db.Close(); err != nil {
				b.Fatal(err)
			}
			for _, mode := range []struct {
				name  string
				setup func(b *testing.B)
			}{
				{"cached", nil},
				{"rebuild", func(b *testing.B) {
					saved := sidecar + ".saved"
					if err := os.Rename(sidecar, saved); err != nil {
						b.Fatal(err)
					}
					b.Cleanup(func() { _ = os.Rename(saved, sidecar) })
				}},
			} {
				b.Run(mode.name, func(b *testing.B) {
					if mode.setup != nil {
						mode.setup(b)
					}
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						db, err := Open(path, OpenOptions{
							ReadOnly:         true,
							EnableVector:     true,
							VectorDimensions: 16,
							VectorIndexMode:  VectorIndexHNSWSynchronous,
						})
						if err != nil {
							b.Fatal(err)
						}
						if err := db.Close(); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}

// deterministicVector produces a pseudo-random 16-dim vector from a node ID.
// Vectors are not collinear: each component uses a different phase.
func deterministicVector(id uint64, dims int) []float32 {
	vec := make([]float32, dims)
	for d := range vec {
		// Simple hash-like mix per dimension to avoid collinearity.
		h := id*uint64(d+1)*2654435761 & 0xFFFFFFFF
		vec[d] = float32(h&0xFFFF)/float32(0xFFFF) - 0.5
	}
	return vec
}
