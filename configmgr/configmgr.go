package configmgr

import (
	"context"

	"github.com/lancsnet/multi-region/proto"
)

type Broadcaster interface {
	Broadcast(cfg *proto.ConfigPayload) error
}

type ConfigDistributor interface {
	Distribute(ctx context.Context, cfg *proto.ConfigPayload) error
	OnConfigUpdate(handler func(*proto.ConfigPayload))
}

type Distributor struct {
	broadcaster Broadcaster
	handler     func(*proto.ConfigPayload)
}

func NewDistributor(b Broadcaster) *Distributor {
	return &Distributor{broadcaster: b}
}

// Distribute pushes cfg to all directly-connected children via the
// underlying Broadcaster. Each child's own Distributor.HandleIncoming is
// what triggers Distribute again on that child, producing recursive
// propagation down the tree.
func (d *Distributor) Distribute(ctx context.Context, cfg *proto.ConfigPayload) error {
	return d.broadcaster.Broadcast(cfg)
}

func (d *Distributor) OnConfigUpdate(handler func(*proto.ConfigPayload)) {
	d.handler = handler
}

// HandleIncoming is invoked by node.Node when a ConfigPayload arrives from
// this node's parent (via transport.Client's ConfigHandler). It runs the
// registered handler, which node.Node wires to call Distribute again so the
// update propagates to this node's own children.
func (d *Distributor) HandleIncoming(cfg *proto.ConfigPayload) {
	if d.handler != nil {
		d.handler(cfg)
	}
}
