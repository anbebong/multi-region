package transport

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/anbebong/multi-region/proto"
)

func dialBufconn(t *testing.T, lis *bufconn.Listener) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	return conn
}

func TestServer_ReceivesUpstreamEnvelope(t *testing.T) {
	lis := bufconn.Listen(1024 * 1024)
	received := make(chan *proto.Envelope, 1)

	srv := NewServer(nil, func(ctx context.Context, env *proto.Envelope) error {
		received <- env
		return nil
	})
	grpcServer := grpc.NewServer()
	proto.RegisterNodeServiceServer(grpcServer, srv)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()

	conn := dialBufconn(t, lis)
	defer conn.Close()
	client := proto.NewNodeServiceClient(conn)

	stream, err := client.Upstream(context.Background())
	if err != nil {
		t.Fatalf("Upstream: %v", err)
	}
	err = stream.Send(&proto.Envelope{Id: "1", Kind: "log", Timestamp: 1, Payload: []byte("hi")})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}

	select {
	case env := <-received:
		if env.Id != "1" || env.Kind != "log" {
			t.Fatalf("unexpected envelope: %+v", env)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for upstream envelope on server")
	}
}

func TestServer_DownstreamIndependentOfUpstream(t *testing.T) {
	lis := bufconn.Listen(1024 * 1024)
	srv := NewServer(nil, func(ctx context.Context, env *proto.Envelope) error { return nil })
	grpcServer := grpc.NewServer()
	proto.RegisterNodeServiceServer(grpcServer, srv)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()

	conn := dialBufconn(t, lis)
	defer conn.Close()
	client := proto.NewNodeServiceClient(conn)

	// Open Downstream without ever opening Upstream at all, to prove the
	// two RPCs are fully independent connections.
	downStream, err := client.Downstream(context.Background(), &proto.Ack{})
	if err != nil {
		t.Fatalf("Downstream: %v", err)
	}

	// The server's Downstream handler registers this child asynchronously
	// (server-streaming RPCs don't guarantee the handler has started before
	// the client-side call returns), so retry the broadcast until it
	// reaches at least one registered child.
	deadline := time.Now().Add(2 * time.Second)
	for srv.ChildrenCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := srv.BroadcastDownstream(&proto.Envelope{Id: "2", Kind: "config", Payload: []byte("v1")}); err != nil {
		t.Fatalf("BroadcastDownstream: %v", err)
	}

	recvCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	envCh := make(chan *proto.Envelope, 1)
	errCh := make(chan error, 1)
	go func() {
		e, err := downStream.Recv()
		if err != nil {
			errCh <- err
			return
		}
		envCh <- e
	}()

	var env *proto.Envelope
	select {
	case env = <-envCh:
	case err = <-errCh:
		t.Fatalf("Recv: %v", err)
	case <-recvCtx.Done():
		t.Fatal("timed out waiting for downstream envelope")
	}
	if string(env.Payload) != "v1" {
		t.Fatalf("unexpected envelope: %+v", env)
	}
}
