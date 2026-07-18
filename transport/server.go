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

	"github.com/anbebong/multi-region/auth"
	"github.com/anbebong/multi-region/proto"
)

// UpstreamHandler is invoked for every Envelope a child sends up through its
// stream. The framework does not interpret env.Kind or env.Payload — that is
// entirely up to whatever handler the service using this framework wires up.
type UpstreamHandler func(ctx context.Context, env *proto.Envelope) error

// Authenticator matches auth.Authenticator; declared locally so transport
// stays testable without requiring a real CA (nil Authenticator == insecure).
type Authenticator interface {
	ClientTLSConfig() (*tls.Config, error)
	ServerTLSConfig() (*tls.Config, error)
}

var _ Authenticator = (auth.Authenticator)(nil)

type Server struct {
	proto.UnimplementedNodeServiceServer

	authn      Authenticator
	onUpstream UpstreamHandler

	mu       sync.Mutex
	children map[int64]chan *proto.Envelope
	nextID   int64

	grpcServer *grpc.Server
}

func NewServer(authn Authenticator, onUpstream UpstreamHandler) *Server {
	return &Server{
		authn:      authn,
		onUpstream: onUpstream,
		children:   make(map[int64]chan *proto.Envelope),
	}
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
			log.Printf("[transport] child %d send buffer full, dropped downstream kind=%q", id, env.Kind)
		}
	}
	return nil
}

func (s *Server) Stream(stream proto.NodeService_StreamServer) error {
	id := s.registerChild()
	downstreamCh := s.children[id]
	log.Printf("[transport] child %d connected (%d children now connected)", id, s.ChildrenCount())
	defer func() {
		s.unregisterChild(id)
		log.Printf("[transport] child %d disconnected (%d children now connected)", id, s.ChildrenCount())
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
				log.Printf("[transport] child %d sent upstream id=%s kind=%q", id, env.Id, env.Kind)
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

func (s *Server) registerChild() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	id := s.nextID
	s.children[id] = make(chan *proto.Envelope, 16)
	return id
}

func (s *Server) unregisterChild(id int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.children, id)
}
