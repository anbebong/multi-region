package transport

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	"github.com/anbebong/multi-region/proto"
)

type fixedResolver struct{ addr string }

func (f fixedResolver) ParentAddr() (string, error) { return f.addr, nil }

func TestClient_SendUpstreamAndReceiveDownstream(t *testing.T) {
	lis := bufconn.Listen(1024 * 1024)

	receivedUp := make(chan *proto.Envelope, 1)
	srv := NewServer(nil, func(ctx context.Context, env *proto.Envelope) error {
		receivedUp <- env
		return nil
	})
	grpcServer := grpc.NewServer()
	proto.RegisterNodeServiceServer(grpcServer, srv)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()

	dialer := func(ctx context.Context, addr string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}

	receivedDown := make(chan *proto.Envelope, 1)
	client := NewClient("child-a", fixedResolver{addr: "bufnet"}, nil, func(env *proto.Envelope) {
		receivedDown <- env
	})
	client.dialer = dialer

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer client.Close()

	if err := client.SendUpstream(ctx, &proto.Envelope{Id: "1", Kind: "log", Timestamp: 1}); err != nil {
		t.Fatalf("SendUpstream: %v", err)
	}

	select {
	case env := <-receivedUp:
		if env.Id != "1" {
			t.Fatalf("unexpected envelope: %+v", env)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for server to receive upstream envelope")
	}

	if _, err := srv.BroadcastDownstream(&proto.Envelope{Id: "2", Kind: "config", Payload: []byte("v1")}); err != nil {
		t.Fatalf("BroadcastDownstream: %v", err)
	}

	select {
	case env := <-receivedDown:
		if string(env.Payload) != "v1" {
			t.Fatalf("unexpected envelope: %+v", env)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for client to receive downstream envelope")
	}
}
