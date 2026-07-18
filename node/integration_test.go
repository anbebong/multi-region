package node

import (
	"context"
	"testing"
	"time"

	"github.com/anbebong/multi-region/proto"
	"github.com/anbebong/multi-region/resolver"
)

func TestNode_ThreeTierTopology_UpstreamAndDownstream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Tier 1: Trung tam (root, server only)
	root, err := New(WithID("root"), WithListenAddr("127.0.0.1:19543"))
	if err != nil {
		t.Fatalf("New root: %v", err)
	}
	if err := root.Start(ctx); err != nil {
		t.Fatalf("root.Start: %v", err)
	}
	defer root.Stop()
	time.Sleep(100 * time.Millisecond)

	// Tier 2: Chi nhanh (server + client)
	branch, err := New(
		WithID("branch"),
		WithListenAddr("127.0.0.1:19544"),
		WithResolver(resolver.NewStaticResolver("127.0.0.1:19543")),
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
	leaf, err := New(
		WithID("leaf"),
		WithResolver(resolver.NewStaticResolver("127.0.0.1:19544")),
	)
	if err != nil {
		t.Fatalf("New leaf: %v", err)
	}
	if err := leaf.Start(ctx); err != nil {
		t.Fatalf("leaf.Start: %v", err)
	}
	defer leaf.Stop()

	t.Run("upstream envelope flows from leaf up to root", func(t *testing.T) {
		received := make(chan *proto.Envelope, 1)
		root.OnUpstream("log", func(ctx context.Context, env *proto.Envelope) {
			received <- env
		})

		if err := leaf.SendUp(ctx, "log", []byte("from leaf")); err != nil {
			t.Fatalf("leaf.SendUp: %v", err)
		}

		select {
		case env := <-received:
			if string(env.Payload) != "from leaf" {
				t.Fatalf("expected payload %q, got %q", "from leaf", env.Payload)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("root never received leaf's upstream envelope")
		}
	})

	t.Run("downstream envelope flows from root down to leaf", func(t *testing.T) {
		received := make(chan *proto.Envelope, 1)
		leaf.OnDownstream("config", func(env *proto.Envelope) {
			received <- env
		})

		if err := root.SendDown(ctx, "config", []byte("cfg-v1")); err != nil {
			t.Fatalf("root.SendDown: %v", err)
		}

		select {
		case env := <-received:
			if string(env.Payload) != "cfg-v1" {
				t.Fatalf("expected payload %q, got %q", "cfg-v1", env.Payload)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("leaf never received the downstream envelope pushed from root")
		}
	})
}
