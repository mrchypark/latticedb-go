package conformance

import (
	"path/filepath"
	"reflect"
	"testing"
)

func TestProjectionLiteralPrecedence(t *testing.T) {
	db := openDB(t, filepath.Join(t.TempDir(), "literals.ltdb"), OpenOptions{Create: true})
	for _, tc := range []struct {
		query string
		want  any
	}{
		{"RETURN true AS v", true},
		{"RETURN false AS v", false},
		{"RETURN null AS v", nil},
		{"WITH true AS v RETURN v", true},
		{"WITH null AS v RETURN v", nil},
		{"RETURN count(null) AS v", int64(0)},
		{"RETURN count(true) AS v", int64(1)},
		{"RETURN count(false) AS v", int64(1)},
		{"WITH 7 AS `null` RETURN `null` AS v", int64(7)},
		{"WITH 7 AS `true` RETURN count(`true`) AS v", int64(1)},
	} {
		t.Run(tc.query, func(t *testing.T) {
			result, err := db.Query(tc.query, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Rows) != 1 || !reflect.DeepEqual(result.Rows[0]["v"], tc.want) {
				t.Fatalf("rows = %#v, want v=%#v", result.Rows, tc.want)
			}
		})
	}
}
