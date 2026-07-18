package node

import (
	"context"
	"testing"
	"time"

	"github.com/anbebong/multi-region/proto"
	"github.com/anbebong/multi-region/resolver"
)

func TestNode_ChildQueuesUpstreamWhileParentDown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	root, err := New(WithID("root"), WithListenAddr("127.0.0.1:19643"))
	if err != nil {
		t.Fatalf("New root: %v", err)
	}

	child, err := New(
		WithID("child"),
		WithResolver(resolver.NewStaticResolver("127.0.0.1:19643")),
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
	time.Sleep(200 * time.Millisecond) // let the child notice the parent is gone

	if err := child.SendUp(ctx, "log", []byte("during outage")); err != nil {
		t.Fatalf("child.SendUp should not fail even if forwarding is retried in background: %v", err)
	}

	// Bring root back up on the same address and confirm the entry
	// eventually reaches it (proves the in-memory queue + retry works, not
	// just that the first send was queued).
	root2, err := New(WithID("root"), WithListenAddr("127.0.0.1:19643"))
	if err != nil {
		t.Fatalf("New root2: %v", err)
	}
	received := make(chan *proto.Envelope, 1)
	root2.OnUpstream("log", func(ctx context.Context, env *proto.Envelope) {
		received <- env
	})
	if err := root2.Start(ctx); err != nil {
		t.Fatalf("root2.Start: %v", err)
	}
	defer root2.Stop()

	select {
	case env := <-received:
		if string(env.Payload) != "during outage" {
			t.Fatalf("expected payload %q, got %q", "during outage", env.Payload)
		}
	case <-time.After(25 * time.Second):
		t.Fatal("envelope queued during outage was never forwarded after parent came back")
	}
}
