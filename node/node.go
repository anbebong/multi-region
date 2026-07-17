package node

import (
	"context"
	"fmt"
	"time"

	"github.com/lancsnet/multi-region/configmgr"
	"github.com/lancsnet/multi-region/forwarder"
	"github.com/lancsnet/multi-region/proto"
	"github.com/lancsnet/multi-region/storage"
	"github.com/lancsnet/multi-region/transport"
)

type noopBroadcaster struct{}

func (noopBroadcaster) Broadcast(cfg *proto.ConfigPayload) error { return nil }

type Node struct {
	cfg config

	server      *transport.Server
	client      *transport.Client
	forwarder   forwarder.Forwarder
	distributor *configmgr.Distributor
}

func New(opts ...Option) (*Node, error) {
	var c config
	for _, opt := range opts {
		opt(&c)
	}
	if c.id == "" {
		return nil, fmt.Errorf("node: WithID is required")
	}
	if c.storage == nil {
		return nil, fmt.Errorf("node: WithStorage is required")
	}
	if c.listenAddr == "" && c.resolver == nil {
		return nil, fmt.Errorf("node: at least one of WithListenAddr (accept children) or WithResolver (connect to a parent) is required")
	}
	return &Node{cfg: c}, nil
}

// Start brings the node's capabilities online based purely on which options
// were configured: WithListenAddr makes it accept children, WithResolver
// makes it connect to a parent. A node with both behaves as a Chi nhanh; a
// node with only one behaves as a Trung tam (listen only) or a Leaf
// (resolver only). There is no separate "role" switch — capability follows
// configuration.
func (n *Node) Start(ctx context.Context) error {
	onLog := func(ctx context.Context, entry *proto.LogEntry) error {
		if err := n.cfg.storage.Save(ctx, entry); err != nil {
			return fmt.Errorf("save incoming log: %w", err)
		}
		if n.forwarder != nil {
			return n.forwarder.Forward(ctx, entry)
		}
		return nil
	}

	if n.cfg.listenAddr != "" {
		n.server = transport.NewServer(n.cfg.authn, onLog)
		n.distributor = configmgr.NewDistributor(n.server)
		// A node that also has a parent re-distributes any config it
		// receives down to its own children.
		n.distributor.OnConfigUpdate(func(cfg *proto.ConfigPayload) {
			_ = n.distributor.Distribute(context.Background(), cfg)
		})
		go func() {
			_ = n.server.Listen(n.cfg.listenAddr)
		}()
	}

	if n.cfg.resolver != nil {
		if n.distributor == nil {
			n.distributor = configmgr.NewDistributor(noopBroadcaster{})
		}
		onConfig := func(cfg *proto.ConfigPayload) {
			n.distributor.HandleIncoming(cfg)
		}
		n.client = transport.NewClient(n.cfg.resolver, n.cfg.authn, onConfig)
		if err := n.client.Connect(ctx); err != nil {
			return fmt.Errorf("connect to parent: %w", err)
		}
		n.forwarder = forwarder.NewGRPCForwarder(n.client, 5, time.Second)
		go n.flushLoop(ctx)
	}

	return nil
}

// flushLoop periodically re-queries storage for entries belonging to this
// node and retries forwarding any that a prior attempt could not deliver.
// It is a coarse-grained safety net on top of the forwarder's own per-call
// retries; it does not track per-entry delivery state, so in this minimal
// version it simply re-forwards the node's own entries on an interval.
func (n *Node) flushLoop(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			entries, err := n.cfg.storage.Query(ctx, storage.QueryFilter{NodeID: n.cfg.id})
			if err != nil || n.forwarder == nil {
				continue
			}
			for _, e := range entries {
				_ = n.forwarder.Forward(ctx, e)
			}
		}
	}
}

func (n *Node) Stop() error {
	if n.client != nil {
		n.client.Close()
	}
	if n.server != nil {
		n.server.Stop()
	}
	return nil
}

// Ingest is the entry point for locally-produced log entries (e.g. from an
// agent feeding this node directly). It persists locally and forwards
// upward, exactly like a log entry received from a child.
func (n *Node) Ingest(ctx context.Context, entry *proto.LogEntry) error {
	if err := n.cfg.storage.Save(ctx, entry); err != nil {
		return fmt.Errorf("save ingested log: %w", err)
	}
	if n.forwarder != nil {
		if err := n.forwarder.Forward(ctx, entry); err != nil {
			// Forwarding failed (e.g. parent unreachable); the entry is
			// safely persisted locally and flushLoop will retry it.
			return nil
		}
	}
	return nil
}
