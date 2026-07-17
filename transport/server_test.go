package transport

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/lancsnet/multi-region/proto"
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

func TestServer_ReceivesLogEntry(t *testing.T) {
	lis := bufconn.Listen(1024 * 1024)
	received := make(chan *proto.LogEntry, 1)

	srv := NewServer(nil, func(ctx context.Context, entry *proto.LogEntry) error {
		received <- entry
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
		Body: &proto.StreamMessage_Log{Log: &proto.LogEntry{Id: "1", NodeId: "child-a", Timestamp: 1, Payload: []byte("hi")}},
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}

	select {
	case entry := <-received:
		if entry.Id != "1" || entry.NodeId != "child-a" {
			t.Fatalf("unexpected entry: %+v", entry)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for log entry on server")
	}
}
