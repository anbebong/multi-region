package transport

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/anbebong/multi-region/proto"
	"github.com/anbebong/multi-region/resolver"
)

// DownstreamHandler is invoked for every Envelope this client receives from
// its parent. The framework does not interpret env.Kind or env.Payload.
type DownstreamHandler func(env *proto.Envelope)

type Client struct {
	resolver     resolver.Resolver
	authn        Authenticator
	onDownstream DownstreamHandler

	// dialer overrides the network dialer; nil in production (real TCP/TLS),
	// set to a bufconn dialer in tests.
	dialer func(ctx context.Context, addr string) (net.Conn, error)

	mu      sync.Mutex
	baseCtx context.Context
	conn    *grpc.ClientConn
	stream  proto.NodeService_StreamClient
	cancel  context.CancelFunc
	closed  bool
}

func NewClient(r resolver.Resolver, authn Authenticator, onDownstream DownstreamHandler) *Client {
	return &Client{resolver: r, authn: authn, onDownstream: onDownstream}
}

func (c *Client) Connect(ctx context.Context) error {
	addr, err := c.resolver.ParentAddr()
	if err != nil {
		return fmt.Errorf("resolve parent address: %w", err)
	}

	var creds credentials.TransportCredentials
	if c.authn != nil {
		tlsCfg, err := c.authn.ClientTLSConfig()
		if err != nil {
			return fmt.Errorf("client tls config: %w", err)
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
		return fmt.Errorf("dial parent %s: %w", addr, err)
	}
	log.Printf("[transport] dialed parent at %s", addr)

	c.mu.Lock()
	c.baseCtx = ctx
	c.conn = conn
	c.mu.Unlock()

	return c.reopenStream()
}

// reopenStream opens a fresh Stream RPC on the existing ClientConn and
// starts a new recvLoop for it. The underlying ClientConn keeps trying to
// re-establish its transport on its own (grpc-go's default backoff), so a
// stream broken by a parent outage can be replaced once the transport is
// back, without redialing from scratch.
func (c *Client) reopenStream() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return fmt.Errorf("client is closed")
	}
	conn := c.conn
	baseCtx := c.baseCtx
	if c.cancel != nil {
		c.cancel()
	}
	c.mu.Unlock()

	streamCtx, cancel := context.WithCancel(baseCtx)
	stream, err := proto.NewNodeServiceClient(conn).Stream(streamCtx)
	if err != nil {
		cancel()
		return fmt.Errorf("open stream to parent: %w", err)
	}
	log.Printf("[transport] stream to parent (re)opened")

	c.mu.Lock()
	c.stream = stream
	c.cancel = cancel
	c.mu.Unlock()

	go c.recvLoop(stream)
	return nil
}

func (c *Client) recvLoop(stream proto.NodeService_StreamClient) {
	for {
		msg, err := stream.Recv()
		if err != nil {
			log.Printf("[transport] stream to parent closed: %v", err)
			return
		}
		if env := msg.GetDownstream(); env != nil && c.onDownstream != nil {
			log.Printf("[transport] received downstream id=%s kind=%q from parent", env.Id, env.Kind)
			c.onDownstream(env)
		}
	}
}

// SendUpstream sends env to the parent over the live stream. The framework
// does not interpret env — it is opaque payload the service defined.
func (c *Client) SendUpstream(ctx context.Context, env *proto.Envelope) error {
	c.mu.Lock()
	stream := c.stream
	c.mu.Unlock()
	if stream == nil {
		return fmt.Errorf("client not connected")
	}
	err := stream.Send(&proto.StreamMessage{Direction: &proto.StreamMessage_Upstream{Upstream: env}})
	if err != nil {
		// The old stream is dead (e.g. the parent dropped it during an
		// outage); replace it so the *next* forward attempt has a chance
		// to succeed once the parent comes back.
		if reopenErr := c.reopenStream(); reopenErr != nil {
			return fmt.Errorf("send upstream: %w (reopen also failed: %v)", err, reopenErr)
		}
		return fmt.Errorf("send upstream on stale stream, reopened for next attempt: %w", err)
	}
	return nil
}

// Connected reports whether the client currently has an open stream to the
// parent. Note this reflects the last (re)established stream; a send on a
// stale stream is detected lazily by SendUpstream, which reopens it, so this
// can briefly read true just before a send discovers the stream is actually
// dead.
func (c *Client) Connected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.closed && c.stream != nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	if c.cancel != nil {
		c.cancel()
	}
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
