package node

import (
	"context"
	"fmt"
	"sync"
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

		if _, err := root.SendDown(ctx, "config", []byte("cfg-v1")); err != nil {
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

// TestNode_IntermediateNode_ConcurrentChildrenUpstream simulates the
// scenario of an intermediate node (like TINH between TRUNG-UONG and many
// XA) receiving upstream Envelopes from several children at the same time.
// All of them share the intermediate node's single Client (one Upstream
// connection to its own parent), so this exercises transport.Client.
// SendUpstream's concurrency safety: every child's Envelope must arrive at
// the root intact and exactly once, with no data races (run with -race) and
// no envelope lost or corrupted by concurrent forwarding.
func TestNode_IntermediateNode_ConcurrentChildrenUpstream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	root, err := New(WithID("root"), WithListenAddr("127.0.0.1:19553"))
	if err != nil {
		t.Fatalf("New root: %v", err)
	}

	var mu sync.Mutex
	received := make(map[string]bool)
	root.OnUpstream("log", func(ctx context.Context, env *proto.Envelope) {
		mu.Lock()
		received[string(env.Payload)] = true
		mu.Unlock()
	})

	if err := root.Start(ctx); err != nil {
		t.Fatalf("root.Start: %v", err)
	}
	defer root.Stop()
	time.Sleep(100 * time.Millisecond)

	intermediate, err := New(
		WithID("intermediate"),
		WithListenAddr("127.0.0.1:19554"),
		WithResolver(resolver.NewStaticResolver("127.0.0.1:19553")),
	)
	if err != nil {
		t.Fatalf("New intermediate: %v", err)
	}
	if err := intermediate.Start(ctx); err != nil {
		t.Fatalf("intermediate.Start: %v", err)
	}
	defer intermediate.Stop()
	time.Sleep(100 * time.Millisecond)

	const numChildren = 20
	const perChild = 10
	children := make([]*Node, numChildren)
	for i := range children {
		child, err := New(
			WithID(fmt.Sprintf("child-%d", i)),
			WithResolver(resolver.NewStaticResolver("127.0.0.1:19554")),
		)
		if err != nil {
			t.Fatalf("New child-%d: %v", i, err)
		}
		if err := child.Start(ctx); err != nil {
			t.Fatalf("child-%d.Start: %v", i, err)
		}
		defer child.Stop()
		children[i] = child
	}
	time.Sleep(200 * time.Millisecond) // let all children finish connecting

	var wg sync.WaitGroup
	for i, child := range children {
		for j := 0; j < perChild; j++ {
			wg.Add(1)
			go func(childIdx, msgIdx int, c *Node) {
				defer wg.Done()
				payload := fmt.Sprintf("child-%d-msg-%d", childIdx, msgIdx)
				if err := c.SendUp(ctx, "log", []byte(payload)); err != nil {
					t.Errorf("child-%d SendUp msg %d: %v", childIdx, msgIdx, err)
				}
			}(i, j, child)
		}
	}
	wg.Wait()

	deadline := time.Now().Add(5 * time.Second)
	want := numChildren * perChild
	for {
		mu.Lock()
		got := len(received)
		mu.Unlock()
		if got == want {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("root received %d/%d envelopes before timeout", got, want)
		}
		time.Sleep(50 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	for i := 0; i < numChildren; i++ {
		for j := 0; j < perChild; j++ {
			payload := fmt.Sprintf("child-%d-msg-%d", i, j)
			if !received[payload] {
				t.Errorf("root never received %q", payload)
			}
		}
	}
}
