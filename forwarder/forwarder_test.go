package forwarder

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lancsnet/multi-region/proto"
)

type fakeSender struct {
	failTimes int
	calls     int
}

func (f *fakeSender) SendLog(ctx context.Context, entry *proto.LogEntry) error {
	f.calls++
	if f.calls <= f.failTimes {
		return errors.New("simulated send failure")
	}
	return nil
}

func TestGRPCForwarder_RetriesUntilSuccess(t *testing.T) {
	sender := &fakeSender{failTimes: 2}
	fwd := NewGRPCForwarder(sender, 5, time.Millisecond)

	err := fwd.Forward(context.Background(), &proto.LogEntry{Id: "1"})
	if err != nil {
		t.Fatalf("Forward: %v", err)
	}
	if sender.calls != 3 {
		t.Fatalf("expected 3 calls (2 failures + 1 success), got %d", sender.calls)
	}
}

func TestGRPCForwarder_GivesUpAfterMaxRetries(t *testing.T) {
	sender := &fakeSender{failTimes: 100}
	fwd := NewGRPCForwarder(sender, 3, time.Millisecond)

	err := fwd.Forward(context.Background(), &proto.LogEntry{Id: "1"})
	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}
	if sender.calls != 3 {
		t.Fatalf("expected exactly 3 attempts, got %d", sender.calls)
	}
}
