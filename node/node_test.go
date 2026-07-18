package node

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/anbebong/multi-region/proto"
	"github.com/anbebong/multi-region/resolver"
)

func TestNode_ChildSendsUpstreamToParent(t *testing.T) {
	parent, err := New(WithID("parent"), WithListenAddr("127.0.0.1:19443"))
	if err != nil {
		t.Fatalf("New parent: %v", err)
	}

	received := make(chan *proto.Envelope, 1)
	parent.OnUpstream("greeting", func(ctx context.Context, env *proto.Envelope) {
		received <- env
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := parent.Start(ctx); err != nil {
		t.Fatalf("parent.Start: %v", err)
	}
	defer parent.Stop()
	time.Sleep(100 * time.Millisecond) // let the listener come up

	child, err := New(
		WithID("child"),
		WithResolver(resolver.NewStaticResolver("127.0.0.1:19443")),
	)
	if err != nil {
		t.Fatalf("New child: %v", err)
	}
	if err := child.Start(ctx); err != nil {
		t.Fatalf("child.Start: %v", err)
	}
	defer child.Stop()

	if err := child.SendUp(ctx, "greeting", []byte("hi")); err != nil {
		t.Fatalf("child.SendUp: %v", err)
	}

	select {
	case env := <-received:
		if string(env.Payload) != "hi" {
			t.Fatalf("expected payload %q, got %q", "hi", env.Payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("parent never received the child's upstream envelope")
	}
}

func TestNode_SendToChild_TargetsExactlyOneChild(t *testing.T) {
	parent, err := New(WithID("parent"), WithListenAddr("127.0.0.1:19453"))
	if err != nil {
		t.Fatalf("New parent: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := parent.Start(ctx); err != nil {
		t.Fatalf("parent.Start: %v", err)
	}
	defer parent.Stop()
	time.Sleep(100 * time.Millisecond)

	receivedA := make(chan *proto.Envelope, 1)
	childA, err := New(WithID("child-a"), WithResolver(resolver.NewStaticResolver("127.0.0.1:19453")))
	if err != nil {
		t.Fatalf("New childA: %v", err)
	}
	childA.OnDownstream("param", func(env *proto.Envelope) { receivedA <- env })
	if err := childA.Start(ctx); err != nil {
		t.Fatalf("childA.Start: %v", err)
	}
	defer childA.Stop()

	receivedB := make(chan *proto.Envelope, 1)
	childB, err := New(WithID("child-b"), WithResolver(resolver.NewStaticResolver("127.0.0.1:19453")))
	if err != nil {
		t.Fatalf("New childB: %v", err)
	}
	childB.OnDownstream("param", func(env *proto.Envelope) { receivedB <- env })
	if err := childB.Start(ctx); err != nil {
		t.Fatalf("childB.Start: %v", err)
	}
	defer childB.Stop()
	time.Sleep(100 * time.Millisecond) // let both children register

	if err := parent.SendToChild("child-a", "param", []byte("only for A")); err != nil {
		t.Fatalf("SendToChild: %v", err)
	}

	select {
	case env := <-receivedA:
		if string(env.Payload) != "only for A" {
			t.Fatalf("expected payload %q, got %q", "only for A", env.Payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("child-a never received the targeted envelope")
	}

	select {
	case env := <-receivedB:
		t.Fatalf("child-b should not have received anything, got %+v", env)
	case <-time.After(300 * time.Millisecond):
		// expected: nothing arrives at child-b
	}
}

func TestNode_AuthorizeChild_RejectsDisallowedChild(t *testing.T) {
	parent, err := New(
		WithID("parent"),
		WithListenAddr("127.0.0.1:19463"),
		WithAuthorizeChild(func(ctx context.Context, nodeID string) error {
			if nodeID != "allowed-child" {
				return fmt.Errorf("node-id %q is not allowed", nodeID)
			}
			return nil
		}),
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
	time.Sleep(100 * time.Millisecond)

	rejected, err := New(WithID("rejected-child"), WithResolver(resolver.NewStaticResolver("127.0.0.1:19463")))
	if err != nil {
		t.Fatalf("New rejected: %v", err)
	}
	if err := rejected.Start(ctx); err != nil {
		t.Fatalf("rejected.Start: %v", err)
	}
	defer rejected.Stop()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !rejected.ConnectedToParent() {
			return // connection was refused/dropped by the parent, as expected
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("rejected child appears to still be connected to the parent")
}
