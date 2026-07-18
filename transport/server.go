package transport

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/anbebong/multi-region/auth"
	"github.com/anbebong/multi-region/proto"
)

// PendingChild describes a child that tried to connect and was rejected by
// AuthorizeChild, and has not been approved (successfully connected) since.
// It exists purely so a service can show an admin "who's asking to connect"
// UI without having to build its own tracking on top of AuthorizeChild.
type PendingChild struct {
	NodeID     string
	FirstSeen  time.Time
	LastReason string
}

// UpstreamHandler is invoked for every Envelope a child sends up through its
// Upstream stream. The framework does not interpret env.Kind or
// env.Payload — that is entirely up to whatever handler the service using
// this framework wires up.
type UpstreamHandler func(ctx context.Context, env *proto.Envelope) error

// AuthorizeChild is called once per RPC (both Upstream and Downstream are
// checked independently, since each child opens both as separate
// connections), right when that stream is opened, before it is registered
// to send/receive any Envelope. nodeID is the logical ID the child sent via
// the "node-id" gRPC metadata header when it dialed in (empty if it sent
// none). Returning a non-nil error rejects the connection (it is closed
// immediately, never registered).
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
// call's context. Returns "" if the child sent none.
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

	// mu guards everything below. children only ever holds Downstream
	// registrations — Upstream is client-streaming (no long-lived channel
	// needed per child), so it isn't tracked here beyond authorization.
	mu       sync.Mutex
	children map[string]chan *proto.Envelope
	// anonSeq numbers children that connected without a node-id metadata
	// header, so they still get a unique (if not very useful) map key
	// instead of colliding with each other under the empty string.
	anonSeq int64

	// pending tracks node-ids that were rejected by authorizeChild and have
	// not connected successfully since. Only the first rejection per
	// node-id is recorded — a child retrying the same rejected node-id
	// (transport.Client retries automatically) does not refresh FirstSeen
	// or spam this map with duplicates.
	pending map[string]PendingChild

	grpcServer *grpc.Server
}

func NewServer(authn Authenticator, onUpstream UpstreamHandler) *Server {
	return &Server{
		authn:          authn,
		onUpstream:     onUpstream,
		authorizeChild: noopAuthorizeChild,
		children:       make(map[string]chan *proto.Envelope),
		pending:        make(map[string]PendingChild),
	}
}

// PendingChildren returns node-ids currently rejected by AuthorizeChild and
// not yet connected successfully, ordered by nothing in particular (map
// iteration order) — a service builds an "approve" UI on top of this.
func (s *Server) PendingChildren() []PendingChild {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]PendingChild, 0, len(s.pending))
	for _, p := range s.pending {
		out = append(out, p)
	}
	return out
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

// ChildrenCount returns the number of children currently connected on the
// Downstream RPC (the side that has a long-lived registration).
func (s *Server) ChildrenCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.children)
}

// BroadcastDownstream pushes env to every currently-connected child's
// Downstream stream. The framework does not interpret env — it is opaque
// payload the service defined; each child that receives it (transport.
// Client's downstream handler) is responsible for acting on it and/or
// forwarding it further down its own children.
//
// If a child's send buffer is full (it isn't draining fast enough), env is
// dropped for that child specifically — but unlike earlier versions, this
// is not silent: the dropped node-ids are returned in a *BroadcastResult
// alongside a non-nil error, so the caller finds out and can decide what to
// do (retry, alert, ignore), instead of the framework silently discarding
// data mid-transport.
func (s *Server) BroadcastDownstream(env *proto.Envelope) (*BroadcastResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	log.Printf("[transport] broadcasting downstream kind=%q to %d children", env.Kind, len(s.children))
	result := &BroadcastResult{}
	for id, ch := range s.children {
		select {
		case ch <- env:
			result.Delivered++
		default:
			log.Printf("[transport] child %s send buffer full, dropped downstream kind=%q", id, env.Kind)
			result.Dropped = append(result.Dropped, id)
		}
	}
	if len(result.Dropped) > 0 {
		return result, fmt.Errorf("send buffer full for %d of %d children: %v", len(result.Dropped), len(s.children), result.Dropped)
	}
	return result, nil
}

// BroadcastResult reports the outcome of a BroadcastDownstream call: which
// children (by node-id) the Envelope could not be delivered to because
// their send buffer was full, and how many it was delivered to.
type BroadcastResult struct {
	Delivered int
	Dropped   []string
}

// SendToChild pushes env to exactly one currently-connected child's
// Downstream stream, identified by the node-id it claimed when it opened
// that stream (see AuthorizeChild). Returns an error if no child with that
// ID is currently connected — unlike BroadcastDownstream, there is no
// "child not there, silently skip" behavior, since sending to one specific
// child is only meaningful if that child actually receives it.
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

// Upstream is the client-streaming RPC a child uses to send Envelopes up.
// It runs independently of Downstream (a separate TCP/HTTP2 connection), so
// a slow/blocked Downstream send to this same child can never stall
// Upstream delivery, and vice versa.
func (s *Server) Upstream(stream proto.NodeService_UpstreamServer) error {
	nodeID := childNodeID(stream.Context())
	if err := s.authorize(stream.Context(), nodeID); err != nil {
		return err
	}

	for {
		env, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&proto.Ack{})
		}
		if err != nil {
			return err
		}
		log.Printf("[transport] child (node-id=%q) sent upstream id=%s kind=%q", nodeID, env.Id, env.Kind)
		if s.onUpstream != nil {
			if err := s.onUpstream(stream.Context(), env); err != nil {
				return err
			}
		}
	}
}

// Downstream is the server-streaming RPC a child opens once to receive
// Envelopes pushed down from this node for as long as it stays connected.
// It runs independently of Upstream (a separate TCP/HTTP2 connection).
func (s *Server) Downstream(_ *proto.Ack, stream proto.NodeService_DownstreamServer) error {
	nodeID := childNodeID(stream.Context())
	if err := s.authorize(stream.Context(), nodeID); err != nil {
		return err
	}

	key := s.registerChild(nodeID)
	downstreamCh := s.children[key]
	log.Printf("[transport] child %q connected (%d children now connected)", key, s.ChildrenCount())
	defer func() {
		s.unregisterChild(key)
		log.Printf("[transport] child %q disconnected (%d children now connected)", key, s.ChildrenCount())
	}()

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case env := <-downstreamCh:
			if err := stream.Send(env); err != nil {
				return err
			}
		}
	}
}

// authorize runs the configured AuthorizeChild hook for nodeID, recording
// or clearing a PendingChild entry as appropriate. Used by both Upstream
// and Downstream, since a child's two RPCs are authorized independently.
func (s *Server) authorize(ctx context.Context, nodeID string) error {
	s.mu.Lock()
	authorize := s.authorizeChild
	s.mu.Unlock()
	if err := authorize(ctx, nodeID); err != nil {
		log.Printf("[transport] rejected child connection (node-id=%q): %v", nodeID, err)
		s.recordRejection(nodeID, err)
		return fmt.Errorf("connection rejected: %w", err)
	}
	s.clearRejection(nodeID)
	return nil
}

// recordRejection notes that nodeID was just rejected by authorizeChild.
// Only the first rejection is kept — a child retrying the same node-id
// (transport.Client retries automatically after a rejection) does not
// refresh FirstSeen or overwrite the reason with each retry.
func (s *Server) recordRejection(nodeID string, reason error) {
	if nodeID == "" {
		return // nothing to show an admin without a claimed identity
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.pending[nodeID]; exists {
		return
	}
	s.pending[nodeID] = PendingChild{
		NodeID:     nodeID,
		FirstSeen:  time.Now(),
		LastReason: reason.Error(),
	}
}

// clearRejection removes nodeID from the pending-rejection set, called
// once it successfully authorizes and connects.
func (s *Server) clearRejection(nodeID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.pending, nodeID)
}

// registerChild adds a new Downstream entry keyed by its claimed node-id.
// If nodeID is empty (the child sent no node-id metadata) or already in use
// by another currently-connected child, a unique synthetic key is used
// instead so the connection is still tracked, just not addressable via
// SendToChild.
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
