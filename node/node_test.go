package node

import (
	"context"
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
