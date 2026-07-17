package transport

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	"github.com/lancsnet/multi-region/proto"
)

type fixedResolver struct{ addr string }

func (f fixedResolver) ParentAddr() (string, error) { return f.addr, nil }

func TestClient_SendLogAndReceiveConfig(t *testing.T) {
	lis := bufconn.Listen(1024 * 1024)

	receivedLog := make(chan *proto.LogEntry, 1)
	srv := NewServer(nil, func(ctx context.Context, entry *proto.LogEntry) error {
		receivedLog <- entry
		return nil
	})
	grpcServer := grpc.NewServer()
	proto.RegisterNodeServiceServer(grpcServer, srv)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()

	dialer := func(ctx context.Context, addr string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}

	receivedCfg := make(chan *proto.ConfigPayload, 1)
	client := NewClient(fixedResolver{addr: "bufnet"}, nil, func(cfg *proto.ConfigPayload) {
		receivedCfg <- cfg
	})
	client.dialer = dialer

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer client.Close()

	if err := client.SendLog(ctx, &proto.LogEntry{Id: "1", NodeId: "child-a", Timestamp: 1}); err != nil {
		t.Fatalf("SendLog: %v", err)
	}

	select {
	case entry := <-receivedLog:
		if entry.Id != "1" {
			t.Fatalf("unexpected entry: %+v", entry)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for server to receive log")
	}

	if err := srv.Broadcast(&proto.ConfigPayload{Version: "v1"}); err != nil {
		t.Fatalf("Broadcast: %v", err)
	}

	select {
	case cfg := <-receivedCfg:
		if cfg.Version != "v1" {
			t.Fatalf("unexpected config: %+v", cfg)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for client to receive config")
	}
}
