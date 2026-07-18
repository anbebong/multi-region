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

	stream, err := client.Stream(context.Background())
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	err = stream.Send(&proto.StreamMessage{
		Direction: &proto.StreamMessage_Upstream{Upstream: &proto.Envelope{Id: "1", Kind: "log", Timestamp: 1, Payload: []byte("hi")}},
	})
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
