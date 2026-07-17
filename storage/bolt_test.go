package storage

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/lancsnet/multi-region/proto"
)

func TestBoltStorage_SaveAndQuery(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	s, err := NewBoltStorage(dbPath)
	if err != nil {
		t.Fatalf("NewBoltStorage: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	entry := &proto.LogEntry{Id: "1", NodeId: "node-a", Timestamp: 100, Payload: []byte("hello")}
	if err := s.Save(ctx, entry); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, err := s.Query(ctx, QueryFilter{NodeID: "node-a"})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(got) != 1 || got[0].Id != "1" {
		t.Fatalf("expected 1 entry with id=1, got %+v", got)
	}
}

func TestBoltStorage_Delete(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	s, err := NewBoltStorage(dbPath)
	if err != nil {
		t.Fatalf("NewBoltStorage: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	entry := &proto.LogEntry{Id: "1", NodeId: "node-a", Timestamp: 100, Payload: []byte("hello")}
	if err := s.Save(ctx, entry); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := s.Delete(ctx, []string{"1"}); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	got, err := s.Query(ctx, QueryFilter{NodeID: "node-a"})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected 0 entries after delete, got %d", len(got))
	}
}
