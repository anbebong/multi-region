package transport

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/anbebong/multi-region/auth"
	"github.com/anbebong/multi-region/proto"
)

// UpstreamHandler is invoked for every Envelope a child sends up through its
// stream. The framework does not interpret env.Kind or env.Payload — that is
// entirely up to whatever handler the service using this framework wires up.
type UpstreamHandler func(ctx context.Context, env *proto.Envelope) error

// AuthorizeChild is called once, right when a child's stream is opened,
// before the child is registered to receive downstream Envelopes or have
// its upstream Envelopes handled. nodeID is the logical ID the child sent
// via the "node-id" gRPC metadata header when it dialed in (empty if it
// sent none). Returning a non-nil error rejects the connection (it is
// closed immediately, the child is never registered).
//
// The framework does not decide what "authorized" means — it only provides
// the hook, the point in the connection lifecycle where it runs, and the
// child's claimed nodeID. A service typically checks nodeID against its own
// allow-list/database, and/or pulls the child's mTLS client certificate out
// of ctx via peer.FromContext(ctx).AuthInfo.(credentials.TLSInfo) to verify
// nodeID against the certificate's CommonName/SAN.
type AuthorizeChild func(ctx context.Context, nodeID string) error

func noopAuthorizeChild(ctx context.Context, nodeID string) error { return nil }

// childNodeID extracts the claimed node-id metadata header from an incoming
// stream's context. Returns "" if the child sent none.
func childNodeID(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	values := md.Get(nodeIDMetadataKey)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

// Authenticator matches auth.Authenticator; declared locally so transport
// stays testable without requiring a real CA (nil Authenticator == insecure).
type Authenticator interface {
	ClientTLSConfig() (*tls.Config, error)
	ServerTLSConfig() (*tls.Config, error)
}

var _ Authenticator = (auth.Authenticator)(nil)

type Server struct {
	proto.UnimplementedNodeServiceServer

	authn          Authenticator
	onUpstream     UpstreamHandler
	authorizeChild AuthorizeChild

	mu       sync.Mutex
	children map[string]chan *proto.Envelope
	// anonSeq numbers children that connected without a node-id metadata
	// header, so they still get a unique (if not very useful) map key
	// instead of colliding with each other under the empty string.
	anonSeq int64

	grpcServer *grpc.Server
}

func NewServer(authn Authenticator, onUpstream UpstreamHandler) *Server {
	return &Server{
		authn:          authn,
		onUpstream:     onUpstream,
		authorizeChild: noopAuthorizeChild,
		children:       make(map[string]chan *proto.Envelope),
	}
}

// SetAuthorizeChild installs a hook to approve or reject each incoming
// child connection. See the AuthorizeChild type for what it receives and
// when it runs. Passing nil restores the default (accept every child whose
// mTLS handshake already succeeded, i.e. no additional check).
func (s *Server) SetAuthorizeChild(authorize AuthorizeChild) {
	if authorize == nil {
		authorize = noopAuthorizeChild
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.authorizeChild = authorize
}

func (s *Server) Listen(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}

	var creds credentials.TransportCredentials
	if s.authn != nil {
		tlsCfg, err := s.authn.ServerTLSConfig()
		if err != nil {
			return fmt.Errorf("server tls config: %w", err)
		}
		creds = credentials.NewTLS(tlsCfg)
	} else {
		creds = insecure.NewCredentials()
	}

	s.grpcServer = grpc.NewServer(grpc.Creds(creds))
	proto.RegisterNodeServiceServer(s.grpcServer, s)
	return s.grpcServer.Serve(lis)
}

// Stop terminates the server immediately, forcibly closing any in-flight
// child streams. This is intentional: GracefulStop would block until every
// connected child disconnects on its own, which never happens for children
// that keep retrying a live stream — exactly the outage scenario this
// framework needs Stop to simulate/support.
func (s *Server) Stop() {
	if s.grpcServer != nil {
		s.grpcServer.Stop()
	}
}

// ChildrenCount returns the number of currently-connected child streams.
func (s *Server) ChildrenCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.children)
}

// BroadcastDownstream pushes env to every currently-connected child stream.
// The framework does not interpret env — it is opaque payload the service
// defined; each child that receives it (transport.Client's downstream
// handler) is responsible for acting on it and/or forwarding it further
// down its own children.
func (s *Server) BroadcastDownstream(env *proto.Envelope) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	log.Printf("[transport] broadcasting downstream kind=%q to %d children", env.Kind, len(s.children))
	for id, ch := range s.children {
		select {
		case ch <- env:
		default:
			log.Printf("[transport] child %s send buffer full, dropped downstream kind=%q", id, env.Kind)
		}
	}
	return nil
}

// SendToChild pushes env to exactly one currently-connected child,
// identified by the node-id it claimed when it opened its stream (see
// AuthorizeChild). Returns an error if no child with that ID is currently
// connected — unlike BroadcastDownstream, there is no "child not there,
// silently skip" behavior, since sending to one specific child is only
// meaningful if that child actually receives it.
func (s *Server) SendToChild(nodeID string, env *proto.Envelope) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ch, ok := s.children[nodeID]
	if !ok {
		return fmt.Errorf("no child with node-id %q currently connected", nodeID)
	}
	select {
	case ch <- env:
		return nil
	default:
		return fmt.Errorf("send buffer full for child %q, dropped downstream kind=%q", nodeID, env.Kind)
	}
}

func (s *Server) Stream(stream proto.NodeService_StreamServer) error {
	nodeID := childNodeID(stream.Context())

	s.mu.Lock()
	authorize := s.authorizeChild
	s.mu.Unlock()
	if err := authorize(stream.Context(), nodeID); err != nil {
		log.Printf("[transport] rejected child connection (node-id=%q): %v", nodeID, err)
		return fmt.Errorf("connection rejected: %w", err)
	}

	key := s.registerChild(nodeID)
	downstreamCh := s.children[key]
	log.Printf("[transport] child %q connected (%d children now connected)", key, s.ChildrenCount())
	defer func() {
		s.unregisterChild(key)
		log.Printf("[transport] child %q disconnected (%d children now connected)", key, s.ChildrenCount())
	}()

	errCh := make(chan error, 1)
	go func() {
		for {
			msg, err := stream.Recv()
			if err == io.EOF {
				errCh <- nil
				return
			}
			if err != nil {
				errCh <- err
				return
			}
			if env := msg.GetUpstream(); env != nil && s.onUpstream != nil {
				log.Printf("[transport] child %q sent upstream id=%s kind=%q", key, env.Id, env.Kind)
				if err := s.onUpstream(stream.Context(), env); err != nil {
					errCh <- err
					return
				}
			}
		}
	}()

	for {
		select {
		case err := <-errCh:
			return err
		case env := <-downstreamCh:
			if err := stream.Send(&proto.StreamMessage{Direction: &proto.StreamMessage_Downstream{Downstream: env}}); err != nil {
				return err
			}
		}
	}
}

// registerChild adds a new child entry keyed by its claimed node-id. If
// nodeID is empty (the child sent no node-id metadata) or already in use by
// another currently-connected child, a unique synthetic key is used instead
// so the connection is still tracked, just not addressable via SendToChild.
func (s *Server) registerChild(nodeID string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := nodeID
	if key == "" {
		s.anonSeq++
		key = fmt.Sprintf("(anonymous-%d)", s.anonSeq)
	} else if _, exists := s.children[key]; exists {
		s.anonSeq++
		key = fmt.Sprintf("%s(dup-%d)", nodeID, s.anonSeq)
	}
	s.children[key] = make(chan *proto.Envelope, 16)
	return key
}

func (s *Server) unregisterChild(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.children, key)
}
