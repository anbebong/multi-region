package transport

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/anbebong/multi-region/proto"
	"github.com/anbebong/multi-region/resolver"
)

// reconnectInterval is how often Client checks whether its Upstream or
// Downstream RPC to the parent has gone away (e.g. the parent rejected the
// connection via AuthorizeChild, or a network outage closed it) and retries
// opening a new one — regardless of whether anything is being sent upstream
// at the time. Without this, a child rejected for having no approval yet
// would never notice once it's later approved unless something happened to
// call SendUpstream. Upstream and Downstream reconnect independently of
// each other, since they are now separate RPCs/connections.
const reconnectInterval = 10 * time.Second

// nodeIDMetadataKey is the gRPC metadata key a child sends its own logical
// node ID under when opening either RPC, so the parent's transport.Server
// can track children by that ID instead of an opaque connection counter.
const nodeIDMetadataKey = "node-id"

// DownstreamHandler is invoked for every Envelope this client receives on
// its Downstream stream from the parent. The framework does not interpret
// env.Kind or env.Payload.
type DownstreamHandler func(env *proto.Envelope)

// Client holds two independent RPCs to the parent: Upstream (client-
// streaming, this client sends) and Downstream (server-streaming, this
// client receives). They run over separate TCP/HTTP2 connections so a
// slow/blocked send on one can never stall delivery on the other.
type Client struct {
	nodeID       string
	resolver     resolver.Resolver
	authn        Authenticator
	onDownstream DownstreamHandler

	// dialer overrides the network dialer; nil in production (real TCP/TLS),
	// set to a bufconn dialer in tests.
	dialer func(ctx context.Context, addr string) (net.Conn, error)

	baseCtx context.Context
	closed  bool

	mu             sync.Mutex
	upstreamConn   *grpc.ClientConn
	upstreamStream proto.NodeService_UpstreamClient
	// sendMu serializes calls to upstreamStream.Send. grpc.ClientStream is
	// not safe for concurrent Send calls from multiple goroutines — an
	// intermediate node forwarding Envelopes from many children at once
	// all share this one Client, so without this lock their sends could
	// interleave and corrupt the stream. mu alone is not enough: it only
	// protects reading the *stream* pointer, not the Send call itself,
	// which can block on the network for the duration of the call.
	sendMu sync.Mutex

	downstreamConn   *grpc.ClientConn
	downstreamStream proto.NodeService_DownstreamClient
	downstreamCancel context.CancelFunc
}

func NewClient(nodeID string, r resolver.Resolver, authn Authenticator, onDownstream DownstreamHandler) *Client {
	return &Client{nodeID: nodeID, resolver: r, authn: authn, onDownstream: onDownstream}
}

// Connect dials the parent and opens both the Upstream and Downstream
// RPCs. If either fails initially (e.g. this node-id is not yet approved by
// the parent's AuthorizeChild policy), Connect still returns nil — the
// respective reconnect loop keeps retrying in the background, exactly as it
// does for a stream that dies later.
func (c *Client) Connect(ctx context.Context) error {
	c.mu.Lock()
	c.baseCtx = ctx
	c.mu.Unlock()

	if err := c.reopenUpstream(); err != nil {
		log.Printf("[transport] initial upstream open failed, will keep retrying: %v", err)
	}
	if err := c.reopenDownstream(); err != nil {
		log.Printf("[transport] initial downstream open failed, will keep retrying: %v", err)
	}
	go c.reconnectLoop(ctx)
	return nil
}

func (c *Client) dialParent(ctx context.Context) (*grpc.ClientConn, error) {
	addr, err := c.resolver.ParentAddr()
	if err != nil {
		return nil, fmt.Errorf("resolve parent address: %w", err)
	}

	var creds credentials.TransportCredentials
	if c.authn != nil {
		tlsCfg, err := c.authn.ClientTLSConfig()
		if err != nil {
			return nil, fmt.Errorf("client tls config: %w", err)
		}
		creds = credentials.NewTLS(tlsCfg)
	} else {
		creds = insecure.NewCredentials()
	}

	opts := []grpc.DialOption{grpc.WithTransportCredentials(creds)}
	target := addr
	if c.dialer != nil {
		opts = append(opts, grpc.WithContextDialer(c.dialer))
		target = "passthrough:///" + addr
	}

	conn, err := grpc.NewClient(target, opts...)
	if err != nil {
		return nil, fmt.Errorf("dial parent %s: %w", addr, err)
	}
	return conn, nil
}

func (c *Client) withNodeID(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, nodeIDMetadataKey, c.nodeID)
}

// reopenUpstream (re)dials the parent and opens a fresh Upstream RPC,
// replacing any previous one. Independent of Downstream.
func (c *Client) reopenUpstream() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return fmt.Errorf("client is closed")
	}
	baseCtx := c.baseCtx
	c.mu.Unlock()

	conn, err := c.dialParent(baseCtx)
	if err != nil {
		return err
	}

	stream, err := proto.NewNodeServiceClient(conn).Upstream(c.withNodeID(baseCtx))
	if err != nil {
		conn.Close()
		return fmt.Errorf("open upstream to parent: %w", err)
	}
	log.Printf("[transport] upstream to parent (re)opened")

	c.mu.Lock()
	if c.upstreamConn != nil {
		c.upstreamConn.Close()
	}
	c.upstreamConn = conn
	c.upstreamStream = stream
	c.mu.Unlock()
	return nil
}

// reopenDownstream (re)dials the parent and opens a fresh Downstream RPC,
// replacing any previous one, and starts a new recvLoop for it. Independent
// of Upstream.
func (c *Client) reopenDownstream() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return fmt.Errorf("client is closed")
	}
	baseCtx := c.baseCtx
	prevCancel := c.downstreamCancel
	c.mu.Unlock()

	if prevCancel != nil {
		prevCancel()
	}

	conn, err := c.dialParent(baseCtx)
	if err != nil {
		return err
	}

	streamCtx, cancel := context.WithCancel(c.withNodeID(baseCtx))
	stream, err := proto.NewNodeServiceClient(conn).Downstream(streamCtx, &proto.Ack{})
	if err != nil {
		cancel()
		conn.Close()
		return fmt.Errorf("open downstream to parent: %w", err)
	}
	log.Printf("[transport] downstream from parent (re)opened")

	c.mu.Lock()
	if c.downstreamConn != nil {
		c.downstreamConn.Close()
	}
	c.downstreamConn = conn
	c.downstreamStream = stream
	c.downstreamCancel = cancel
	c.mu.Unlock()

	go c.recvLoop(stream)
	return nil
}

// reconnectLoop periodically checks whether Upstream and/or Downstream
// currently have no live connection — because the initial attempt was
// rejected, the parent restarted, or a network outage closed it — and
// retries opening a fresh one for whichever side needs it. The two sides
// are reconnected independently: one being down does not wait on the other.
func (c *Client) reconnectLoop(ctx context.Context) {
	ticker := time.NewTicker(reconnectInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.mu.Lock()
			closed := c.closed
			needsUpstream := c.upstreamStream == nil
			needsDownstream := c.downstreamStream == nil
			c.mu.Unlock()
			if closed {
				return
			}
			if needsUpstream {
				if err := c.reopenUpstream(); err != nil {
					log.Printf("[transport] upstream reconnect attempt failed, will retry: %v", err)
				}
			}
			if needsDownstream {
				if err := c.reopenDownstream(); err != nil {
					log.Printf("[transport] downstream reconnect attempt failed, will retry: %v", err)
				}
			}
		}
	}
}

func (c *Client) recvLoop(stream proto.NodeService_DownstreamClient) {
	for {
		env, err := stream.Recv()
		if err != nil {
			log.Printf("[transport] downstream from parent closed: %v", err)
			c.mu.Lock()
			// Only clear if it's still this recvLoop's stream — a newer
			// reopenDownstream call may have already replaced it, and we
			// must not clobber that newer, live stream.
			if c.downstreamStream == stream {
				c.downstreamStream = nil
			}
			c.mu.Unlock()
			return
		}
		if c.onDownstream != nil {
			log.Printf("[transport] received downstream id=%s kind=%q from parent", env.Id, env.Kind)
			c.onDownstream(env)
		}
	}
}

// SendUpstream sends env to the parent over the live Upstream stream. The
// framework does not interpret env — it is opaque payload the service
// defined. This never blocks on or is blocked by Downstream, since they are
// separate connections.
//
// Safe to call concurrently from multiple goroutines — e.g. an
// intermediate node forwarding Envelopes received from several children at
// once, all through this same Client. sendMu serializes the actual Send
// call (grpc.ClientStream.Send is not safe for concurrent use), so
// concurrent callers queue up here rather than racing on the stream.
func (c *Client) SendUpstream(ctx context.Context, env *proto.Envelope) error {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()

	c.mu.Lock()
	stream := c.upstreamStream
	c.mu.Unlock()
	if stream == nil {
		// No live stream (e.g. recvLoop detected the previous one died).
		// Try to open a fresh one — the parent may be back by now — before
		// giving up.
		if err := c.reopenUpstream(); err != nil {
			return fmt.Errorf("client not connected: %w", err)
		}
		c.mu.Lock()
		stream = c.upstreamStream
		c.mu.Unlock()
	}
	err := stream.Send(env)
	if err != nil {
		// The old stream is dead (e.g. the parent dropped it during an
		// outage); replace it so the *next* forward attempt has a chance
		// to succeed once the parent comes back.
		if reopenErr := c.reopenUpstream(); reopenErr != nil {
			return fmt.Errorf("send upstream: %w (reopen also failed: %v)", err, reopenErr)
		}
		return fmt.Errorf("send upstream on stale stream, reopened for next attempt: %w", err)
	}
	return nil
}

// Connected reports whether the client currently has both a live Upstream
// and Downstream connection to the parent. Note this reflects the last
// (re)established streams; a send on a stale upstream is detected lazily by
// SendUpstream, which reopens it, so this can briefly read true just before
// a send discovers the stream is actually dead.
func (c *Client) Connected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.closed && c.upstreamStream != nil && c.downstreamStream != nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	if c.downstreamCancel != nil {
		c.downstreamCancel()
	}
	var firstErr error
	if c.upstreamConn != nil {
		if err := c.upstreamConn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if c.downstreamConn != nil {
		if err := c.downstreamConn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
