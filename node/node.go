// Package node is the framework core: it owns connecting to a parent,
// accepting children, and moving opaque Envelope data in both directions
// between them. It has no notion of "log" or "config" — those are examples
// a service built on this framework chooses to define via Kind strings and
// its own payload encoding. The framework's only job is the mechanism:
// establish/hold/reconnect connections, and reliably move whatever bytes the
// service hands it, in both directions, recursively across the tree.
package node

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/anbebong/multi-region/proto"
	"github.com/anbebong/multi-region/transport"
)

// UpstreamHandler is called for every Envelope this node receives, whether
// from a child (via transport.Server) or produced locally (via SendUp). The
// framework does not interpret env.Kind or env.Payload.
type UpstreamHandler func(ctx context.Context, env *proto.Envelope)

// DownstreamHandler is called for every Envelope this node receives from its
// parent (via transport.Client). The framework does not interpret env.Kind
// or env.Payload.
type DownstreamHandler func(env *proto.Envelope)

type Node struct {
	cfg config

	server *transport.Server
	client *transport.Client

	mu                 sync.Mutex
	upstreamHandlers   map[string][]UpstreamHandler
	downstreamHandlers map[string][]DownstreamHandler

	pending   pendingQueue
	retryOnce sync.Once
}

func New(opts ...Option) (*Node, error) {
	var c config
	for _, opt := range opts {
		opt(&c)
	}
	if c.id == "" {
		return nil, fmt.Errorf("node: WithID is required")
	}
	if c.listenAddr == "" && c.resolver == nil {
		return nil, fmt.Errorf("node: at least one of WithListenAddr (accept children) or WithResolver (connect to a parent) is required")
	}
	return &Node{
		cfg:                 c,
		upstreamHandlers:    make(map[string][]UpstreamHandler),
		downstreamHandlers:  make(map[string][]DownstreamHandler),
	}, nil
}

// OnUpstream registers handler to be called whenever an Envelope of the
// given kind arrives from a child, or is sent locally via SendUp. The
// service defines kind and payload encoding entirely on its own — the
// framework never inspects them itself.
func (n *Node) OnUpstream(kind string, handler UpstreamHandler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.upstreamHandlers[kind] = append(n.upstreamHandlers[kind], handler)
}

// OnDownstream registers handler to be called whenever an Envelope of the
// given kind arrives from this node's parent.
func (n *Node) OnDownstream(kind string, handler DownstreamHandler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.downstreamHandlers[kind] = append(n.downstreamHandlers[kind], handler)
}

// Start brings the node's capabilities online based purely on which options
// were configured: WithListenAddr makes it accept children, WithResolver
// makes it connect to a parent. A node with both behaves as a Chi nhanh; a
// node with only one behaves as a Trung tam (listen only) or a Leaf
// (resolver only). There is no separate "role" switch — capability follows
// configuration.
func (n *Node) Start(ctx context.Context) error {
	if n.cfg.listenAddr != "" {
		n.server = transport.NewServer(n.cfg.authn, n.handleUpstreamFromChild)
		if n.cfg.authorizeChild != nil {
			n.server.SetAuthorizeChild(n.cfg.authorizeChild)
		}
		go func() {
			_ = n.server.Listen(n.cfg.listenAddr)
		}()
		log.Printf("[node %s] listening for children on %s", n.cfg.id, n.cfg.listenAddr)
	}

	if n.cfg.resolver != nil {
		n.client = transport.NewClient(n.cfg.id, n.cfg.resolver, n.cfg.authn, n.handleDownstreamFromParent)
		if err := n.client.Connect(ctx); err != nil {
			return fmt.Errorf("connect to parent: %w", err)
		}
		log.Printf("[node %s] connected to parent", n.cfg.id)
		n.retryOnce.Do(func() { go n.retryLoop(ctx) })
	}

	return nil
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

// ID returns this node's identifier.
func (n *Node) ID() string { return n.cfg.id }

// ListenAddr returns the address this node accepts children on, or "" if it
// has none (i.e. it is a leaf).
func (n *Node) ListenAddr() string { return n.cfg.listenAddr }

// HasParent reports whether this node is configured to connect up to a
// parent (regardless of whether that connection is currently established).
func (n *Node) HasParent() bool { return n.cfg.resolver != nil }

// ConnectedToParent reports whether this node currently has a live
// connection to its parent. Always false for a node with no parent
// configured.
func (n *Node) ConnectedToParent() bool {
	return n.client != nil && n.client.Connected()
}

// ChildrenCount returns the number of children currently connected to this
// node. Always 0 for a node with no listen address configured.
func (n *Node) ChildrenCount() int {
	if n.server == nil {
		return 0
	}
	return n.server.ChildrenCount()
}

// SendDown pushes an Envelope of the given kind down to this node's
// directly-connected children (each of which recursively forwards it
// further down its own children). It is a no-op if this node has no
// children currently connected.
func (n *Node) SendDown(ctx context.Context, kind string, payload []byte) error {
	if n.server == nil {
		return nil
	}
	env := newEnvelope(n.cfg.id, kind, payload)
	return n.server.BroadcastDownstream(env)
}

// SendToChild pushes an Envelope of the given kind to exactly one of this
// node's directly-connected children, identified by the node-id it claimed
// when it connected (see transport.AuthorizeChild). Unlike SendDown, this
// does not broadcast — it targets a single child, and returns an error if
// no child with that ID is currently connected. It does not itself
// propagate further down that child's own children; if that is desired,
// the receiving node's own kind handler is responsible for calling SendDown
// on its side.
func (n *Node) SendToChild(childID, kind string, payload []byte) error {
	if n.server == nil {
		return fmt.Errorf("node %s has no children (WithListenAddr not set)", n.cfg.id)
	}
	env := newEnvelope(n.cfg.id, kind, payload)
	return n.server.SendToChild(childID, env)
}

// SendUp is the entry point for locally-produced data (e.g. a service's own
// logic calling this on an agent-facing node). It runs this node's own
// upstream handlers for kind, then forwards the Envelope to the parent if
// one is configured. If the parent is unreachable, the Envelope is queued
// in memory and retried by retryLoop once the parent connection recovers —
// this in-memory queue is a transport-level retry mechanism only; it is not
// durable storage, and is lost if the process exits before delivery. A
// service that needs delivery to survive a process restart must persist the
// data itself before calling SendUp.
func (n *Node) SendUp(ctx context.Context, kind string, payload []byte) error {
	env := newEnvelope(n.cfg.id, kind, payload)
	n.runUpstreamHandlers(ctx, env)
	return n.forwardUp(ctx, env)
}

// handleUpstreamFromChild is wired as transport.Server's UpstreamHandler: it
// runs for every Envelope a child sends. This node runs its own handlers for
// it, then keeps forwarding it up (recursion up the tree happens because
// forwardUp calls this same node's client, whose parent does the same).
func (n *Node) handleUpstreamFromChild(ctx context.Context, env *proto.Envelope) error {
	log.Printf("[node %s] received upstream from child: id=%s kind=%q", n.cfg.id, env.Id, env.Kind)
	n.runUpstreamHandlers(ctx, env)
	return n.forwardUp(ctx, env)
}

// handleDownstreamFromParent is wired as transport.Client's
// DownstreamHandler: it runs for every Envelope this node's parent sends.
// This node runs its own handlers for it, then re-broadcasts it to its own
// children, producing recursive propagation down the tree.
func (n *Node) handleDownstreamFromParent(env *proto.Envelope) {
	log.Printf("[node %s] received downstream from parent: id=%s kind=%q", n.cfg.id, env.Id, env.Kind)
	n.runDownstreamHandlers(env)
	if n.server != nil {
		_ = n.server.BroadcastDownstream(env)
	}
}

func (n *Node) runUpstreamHandlers(ctx context.Context, env *proto.Envelope) {
	n.mu.Lock()
	handlers := append([]UpstreamHandler(nil), n.upstreamHandlers[env.Kind]...)
	n.mu.Unlock()
	for _, h := range handlers {
		h(ctx, env)
	}
}

func (n *Node) runDownstreamHandlers(env *proto.Envelope) {
	n.mu.Lock()
	handlers := append([]DownstreamHandler(nil), n.downstreamHandlers[env.Kind]...)
	n.mu.Unlock()
	for _, h := range handlers {
		h(env)
	}
}

// forwardUp sends env to the parent if this node has one. On failure (e.g.
// parent unreachable), env is queued in memory for retryLoop instead of
// returning an error — the caller (SendUp / handleUpstreamFromChild) already
// ran its handlers, so from the framework's point of view the data has been
// "accepted", just not yet delivered further up.
func (n *Node) forwardUp(ctx context.Context, env *proto.Envelope) error {
	if n.client == nil {
		return nil
	}
	if err := n.client.SendUpstream(ctx, env); err != nil {
		log.Printf("[node %s] forward upstream id=%s failed, queued for retry: %v", n.cfg.id, env.Id, err)
		n.pending.push(env)
	}
	return nil
}

// retryLoop is the transport-level safety net for envelopes that could not
// be forwarded upstream on the first attempt (e.g. the parent was
// unreachable). It only re-attempts envelopes still sitting in the
// in-memory pending queue — anything already delivered is removed from the
// queue at send time, so this does not resend indefinitely.
func (n *Node) retryLoop(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			envs := n.pending.drain()
			if len(envs) == 0 {
				continue
			}
			log.Printf("[node %s] retryLoop attempting %d queued envelopes", n.cfg.id, len(envs))
			for _, env := range envs {
				if err := n.client.SendUpstream(ctx, env); err != nil {
					n.pending.push(env)
				}
			}
		}
	}
}
