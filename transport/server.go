package transport

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/lancsnet/multi-region/auth"
	"github.com/lancsnet/multi-region/proto"
)

type LogHandler func(ctx context.Context, entry *proto.LogEntry) error

// Authenticator matches auth.Authenticator; declared locally so transport
// stays testable without requiring a real CA (nil Authenticator == insecure).
type Authenticator interface {
	ClientTLSConfig() (*tls.Config, error)
	ServerTLSConfig() (*tls.Config, error)
}

var _ Authenticator = (auth.Authenticator)(nil)

type Server struct {
	proto.UnimplementedNodeServiceServer

	authn Authenticator
	onLog LogHandler

	mu       sync.Mutex
	children map[int64]chan *proto.ConfigPayload
	nextID   int64

	grpcServer *grpc.Server
}

func NewServer(authn Authenticator, onLog LogHandler) *Server {
	return &Server{
		authn:    authn,
		onLog:    onLog,
		children: make(map[int64]chan *proto.ConfigPayload),
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

// Broadcast pushes cfg to every currently-connected child stream.
func (s *Server) Broadcast(cfg *proto.ConfigPayload) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ch := range s.children {
		select {
		case ch <- cfg:
		default:
			// child's send buffer is full; drop rather than block Broadcast.
		}
	}
	return nil
}

func (s *Server) Stream(stream proto.NodeService_StreamServer) error {
	id := s.registerChild()
	configCh := s.children[id]
	defer s.unregisterChild(id)

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
			if log := msg.GetLog(); log != nil && s.onLog != nil {
				if err := s.onLog(stream.Context(), log); err != nil {
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
		case cfg := <-configCh:
			if err := stream.Send(&proto.StreamMessage{Body: &proto.StreamMessage_Config{Config: cfg}}); err != nil {
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
	s.children[id] = make(chan *proto.ConfigPayload, 16)
	return id
}

func (s *Server) unregisterChild(id int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.children, id)
}
