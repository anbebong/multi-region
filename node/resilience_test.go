package node

import (
	"context"
	"testing"
	"time"

	"github.com/lancsnet/multi-region/proto"
	"github.com/lancsnet/multi-region/resolver"
	"github.com/lancsnet/multi-region/storage"
)

func TestNode_ChildBuffersLogsWhileParentDown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rootDB := storage.MustNewBoltStorage(t)
	root, err := New(WithID("root"), WithListenAddr("127.0.0.1:19643"), WithStorage(rootDB))
	if err != nil {
		t.Fatalf("New root: %v", err)
	}

	childDB := storage.MustNewBoltStorage(t)
	child, err := New(
		WithID("child"),
		WithResolver(resolver.NewStaticResolver("127.0.0.1:19643")),
		WithStorage(childDB),
	)
	if err != nil {
		t.Fatalf("New child: %v", err)
	}

	// Start root first so the child's initial Connect succeeds, then stop it
	// immediately to simulate the parent being unavailable when the child
	// tries to forward.
	if err := root.Start(ctx); err != nil {
		t.Fatalf("root.Start: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if err := child.Start(ctx); err != nil {
		t.Fatalf("child.Start: %v", err)
	}
	defer child.Stop()
	root.Stop()

	entry := &proto.LogEntry{Id: "log-during-outage", NodeId: "child", Timestamp: 1}
	if err := child.Ingest(ctx, entry); err != nil {
		t.Fatalf("child.Ingest should not fail even if forwarding is retried in background: %v", err)
	}

	got, err := childDB.Query(ctx, storage.QueryFilter{NodeID: "child"})
	if err != nil {
		t.Fatalf("Query childDB: %v", err)
	}
	if len(got) != 1 || got[0].Id != "log-during-outage" {
		t.Fatalf("expected the log to remain in local storage during outage, got %+v", got)
	}

	// Bring root back up on the same address and confirm the entry
	// eventually reaches it (proves buffering + recovery, not just local save).
	root2DB := storage.MustNewBoltStorage(t)
	root2, err := New(WithID("root"), WithListenAddr("127.0.0.1:19643"), WithStorage(root2DB))
	if err != nil {
		t.Fatalf("New root2: %v", err)
	}
	if err := root2.Start(ctx); err != nil {
		t.Fatalf("root2.Start: %v", err)
	}
	defer root2.Stop()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		got, _ := root2DB.Query(ctx, storage.QueryFilter{NodeID: "child"})
		if len(got) == 1 && got[0].Id == "log-during-outage" {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("log entry saved during outage was never forwarded after parent came back")
}
