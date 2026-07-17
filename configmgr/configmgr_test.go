package configmgr

import (
	"context"
	"testing"

	"github.com/lancsnet/multi-region/proto"
)

type fakeBroadcaster struct {
	sent []*proto.ConfigPayload
}

func (f *fakeBroadcaster) Broadcast(cfg *proto.ConfigPayload) error {
	f.sent = append(f.sent, cfg)
	return nil
}

func TestDistributor_DistributeCallsBroadcaster(t *testing.T) {
	b := &fakeBroadcaster{}
	d := NewDistributor(b)

	err := d.Distribute(context.Background(), &proto.ConfigPayload{Version: "v1"})
	if err != nil {
		t.Fatalf("Distribute: %v", err)
	}
	if len(b.sent) != 1 || b.sent[0].Version != "v1" {
		t.Fatalf("expected broadcaster to receive v1, got %+v", b.sent)
	}
}

func TestDistributor_OnConfigUpdateInvokesHandler(t *testing.T) {
	b := &fakeBroadcaster{}
	d := NewDistributor(b)

	var received *proto.ConfigPayload
	d.OnConfigUpdate(func(cfg *proto.ConfigPayload) {
		received = cfg
	})
	d.HandleIncoming(&proto.ConfigPayload{Version: "v2"})

	if received == nil || received.Version != "v2" {
		t.Fatalf("expected handler to receive v2, got %+v", received)
	}
}
