package engine

import (
	"fmt"
	"testing"
)

func BenchmarkQueryPropertyIndexOverlayAfterMutation(b *testing.B) {
	for _, size := range []int{10_000, 100_000} {
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			db, err := Open(b.TempDir()+"/overlay.ltdb", OpenOptions{Create: true})
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			if err := db.Update(func(tx *Tx) error {
				for id := 0; id < size; id++ {
					if _, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]any{"k": "hit"}}); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				b.Fatal(err)
			}
			if err := db.CreateNodePropertyIndex("Item", "k"); err != nil {
				b.Fatal(err)
			}
			tx, err := db.Begin(false)
			if err != nil {
				b.Fatal(err)
			}
			defer tx.Rollback()
			query := `MATCH (n:Item) WHERE n.k = "hit" RETURN count(n) AS count`
			b.Run("before", func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := tx.Query(query, nil); err != nil {
						b.Fatal(err)
					}
				}
			})
			if _, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Other"}}); err != nil {
				b.Fatal(err)
			}
			b.Run("after_unrelated_mutation", func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := tx.Query(query, nil); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func BenchmarkQueryPropertyIndexOverlayWith100KPendingUpserts(b *testing.B) {
	db, err := Open(b.TempDir()+"/overlay.ltdb", OpenOptions{Create: true})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		for id := 0; id < 100_000; id++ {
			if _, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]any{"k": "hit"}}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}
	if err := db.CreateNodePropertyIndex("Item", "k"); err != nil {
		b.Fatal(err)
	}
	tx, err := db.Begin(false)
	if err != nil {
		b.Fatal(err)
	}
	defer tx.Rollback()
	for id := 0; id < 100_000; id++ {
		if _, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Other"}}); err != nil {
			b.Fatal(err)
		}
	}
	query := `MATCH (n:Item) WHERE n.k = "hit" RETURN count(n) AS count`
	b.ResetTimer()
	for id := 0; id < b.N; id++ {
		if _, err := tx.Query(query, nil); err != nil {
			b.Fatal(err)
		}
	}
}
