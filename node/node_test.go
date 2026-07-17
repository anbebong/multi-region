package node

import (
	"context"
	"testing"
	"time"

	"github.com/lancsnet/multi-region/proto"
	"github.com/lancsnet/multi-region/resolver"
	"github.com/lancsnet/multi-region/storage"
)

func TestNode_ChildForwardsLogToParent(t *testing.T) {
	parentDB := storage.MustNewBoltStorage(t)
	parent, err := New(
		WithID("parent"),
		WithListenAddr("127.0.0.1:19443"),
		WithStorage(parentDB),
	)
	if err != nil {
		t.Fatalf("New parent: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := parent.Start(ctx); err != nil {
		t.Fatalf("parent.Start: %v", err)
	}
	defer parent.Stop()
	time.Sleep(100 * time.Millisecond) // let the listener come up

	childDB := storage.MustNewBoltStorage(t)
	child, err := New(
		WithID("child"),
		WithResolver(resolver.NewStaticResolver("127.0.0.1:19443")),
		WithStorage(childDB),
	)
	if err != nil {
		t.Fatalf("New child: %v", err)
	}
	if err := child.Start(ctx); err != nil {
		t.Fatalf("child.Start: %v", err)
	}
	defer child.Stop()

	entry := &proto.LogEntry{Id: "log-1", NodeId: "child", Timestamp: 1, Payload: []byte("hi")}
	if err := child.Ingest(ctx, entry); err != nil {
		t.Fatalf("child.Ingest: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got, _ := parentDB.Query(ctx, storage.QueryFilter{NodeID: "child"})
		if len(got) == 1 && got[0].Id == "log-1" {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("parent never received the child's log entry")
}
