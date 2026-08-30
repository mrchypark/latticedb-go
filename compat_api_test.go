package latticedb

import "testing"

func TestReferenceCompatibilityWrappers(t *testing.T) {
	db, err := Open(t.TempDir(), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	write, err := db.BeginWrite()
	if err != nil {
		t.Fatal(err)
	}
	if write.IsReadOnly() || !write.IsActive() {
		t.Fatal("write transaction state mismatch")
	}
	left, err := write.CreateNode(CreateNodeOptions{Labels: []string{"Person"}})
	if err != nil {
		t.Fatal(err)
	}
	right, err := write.CreateNode(CreateNodeOptions{Labels: []string{"Person"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := write.CreateEdge(left.ID, right.ID, "KNOWS", CreateEdgeOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := write.DeleteEdge(left.ID, right.ID, "KNOWS"); err != nil {
		t.Fatal(err)
	}
	if result, err := write.Query("MATCH (n:Person) RETURN id(n) AS id", nil); err != nil || len(result.Rows) != 2 {
		t.Fatalf("transaction query = %#v, %v", result, err)
	}
	if err := write.Commit(); err != nil {
		t.Fatal(err)
	}
	if write.IsActive() {
		t.Fatal("committed transaction remains active")
	}
	ids, err := db.GetNodesByLabel("Person")
	if err != nil || len(ids) != 2 {
		t.Fatalf("label ids = %v, %v", ids, err)
	}
	read, err := db.BeginRead()
	if err != nil {
		t.Fatal(err)
	}
	if !read.IsReadOnly() || !read.IsActive() {
		t.Fatal("read transaction state mismatch")
	}
	if err := read.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := read.Rollback(); err != nil {
		t.Fatalf("second rollback = %v", err)
	}
}
