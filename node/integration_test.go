package node

import (
	"context"
	"testing"
	"time"

	"github.com/lancsnet/multi-region/proto"
	"github.com/lancsnet/multi-region/resolver"
	"github.com/lancsnet/multi-region/storage"
)

func TestNode_ThreeTierTopology_LogUpConfigDown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Tier 1: Trung tam (root, server only)
	rootDB := storage.MustNewBoltStorage(t)
	root, err := New(WithID("root"), WithListenAddr("127.0.0.1:19543"), WithStorage(rootDB))
	if err != nil {
		t.Fatalf("New root: %v", err)
	}
	if err := root.Start(ctx); err != nil {
		t.Fatalf("root.Start: %v", err)
	}
	defer root.Stop()
	time.Sleep(100 * time.Millisecond)

	// Tier 2: Chi nhanh (server + client)
	branchDB := storage.MustNewBoltStorage(t)
	branch, err := New(
		WithID("branch"),
		WithListenAddr("127.0.0.1:19544"),
		WithResolver(resolver.NewStaticResolver("127.0.0.1:19543")),
		WithStorage(branchDB),
	)
	if err != nil {
		t.Fatalf("New branch: %v", err)
	}
	if err := branch.Start(ctx); err != nil {
		t.Fatalf("branch.Start: %v", err)
	}
	defer branch.Stop()
	time.Sleep(100 * time.Millisecond)

	// Tier 3: Leaf (client only)
	leafDB := storage.MustNewBoltStorage(t)
	leaf, err := New(
		WithID("leaf"),
		WithResolver(resolver.NewStaticResolver("127.0.0.1:19544")),
		WithStorage(leafDB),
	)
	if err != nil {
		t.Fatalf("New leaf: %v", err)
	}
	if err := leaf.Start(ctx); err != nil {
		t.Fatalf("leaf.Start: %v", err)
	}
	defer leaf.Stop()

	t.Run("log flows from leaf up to root", func(t *testing.T) {
		entry := &proto.LogEntry{Id: "leaf-log-1", NodeId: "leaf", Timestamp: 1, Payload: []byte("from leaf")}
		if err := leaf.Ingest(ctx, entry); err != nil {
			t.Fatalf("leaf.Ingest: %v", err)
		}

		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			got, _ := rootDB.Query(ctx, storage.QueryFilter{NodeID: "leaf"})
			if len(got) == 1 && got[0].Id == "leaf-log-1" {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatal("root never received leaf's log entry")
	})

	t.Run("config flows from root down to leaf", func(t *testing.T) {
		var leafReceived *proto.ConfigPayload
		leaf.distributor.OnConfigUpdate(func(cfg *proto.ConfigPayload) {
			leafReceived = cfg
		})

		if err := root.distributor.Distribute(ctx, &proto.ConfigPayload{Version: "cfg-v1"}); err != nil {
			t.Fatalf("root distribute: %v", err)
		}

		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if leafReceived != nil && leafReceived.Version == "cfg-v1" {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatal("leaf never received the config pushed from root")
	})
}
