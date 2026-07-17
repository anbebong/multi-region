package transport

import (
	"context"
	"fmt"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/lancsnet/multi-region/proto"
	"github.com/lancsnet/multi-region/resolver"
)

type ConfigHandler func(cfg *proto.ConfigPayload)

type Client struct {
	resolver resolver.Resolver
	authn    Authenticator
	onConfig ConfigHandler

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

func NewClient(r resolver.Resolver, authn Authenticator, onConfig ConfigHandler) *Client {
	return &Client{resolver: r, authn: authn, onConfig: onConfig}
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
			return
		}
		if cfg := msg.GetConfig(); cfg != nil && c.onConfig != nil {
			c.onConfig(cfg)
		}
	}
}

func (c *Client) SendLog(ctx context.Context, entry *proto.LogEntry) error {
	c.mu.Lock()
	stream := c.stream
	c.mu.Unlock()
	if stream == nil {
		return fmt.Errorf("client not connected")
	}
	err := stream.Send(&proto.StreamMessage{Body: &proto.StreamMessage_Log{Log: entry}})
	if err != nil {
		// The old stream is dead (e.g. the parent dropped it during an
		// outage); replace it so the *next* forward attempt has a chance
		// to succeed once the parent comes back.
		if reopenErr := c.reopenStream(); reopenErr != nil {
			return fmt.Errorf("send log: %w (reopen also failed: %v)", err, reopenErr)
		}
		return fmt.Errorf("send log on stale stream, reopened for next attempt: %w", err)
	}
	return nil
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
