package forwarder

import (
	"context"
	"fmt"
	"time"

	"github.com/lancsnet/multi-region/proto"
)

type LogSender interface {
	SendLog(ctx context.Context, entry *proto.LogEntry) error
}

type Forwarder interface {
	Forward(ctx context.Context, entry *proto.LogEntry) error
	Close() error
}

type GRPCForwarder struct {
	sender     LogSender
	maxRetries int
	backoff    time.Duration
}

func NewGRPCForwarder(sender LogSender, maxRetries int, backoff time.Duration) *GRPCForwarder {
	if maxRetries < 1 {
		maxRetries = 1
	}
	return &GRPCForwarder{sender: sender, maxRetries: maxRetries, backoff: backoff}
}

func (f *GRPCForwarder) Forward(ctx context.Context, entry *proto.LogEntry) error {
	var lastErr error
	for attempt := 1; attempt <= f.maxRetries; attempt++ {
		if err := f.sender.SendLog(ctx, entry); err == nil {
			return nil
		} else {
			lastErr = err
		}
		if attempt < f.maxRetries {
			select {
			case <-time.After(f.backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return fmt.Errorf("forward log %s: exhausted %d attempts: %w", entry.Id, f.maxRetries, lastErr)
}

func (f *GRPCForwarder) Close() error {
	return nil
}
