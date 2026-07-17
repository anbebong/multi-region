package storage

import (
	"context"

	"github.com/lancsnet/multi-region/proto"
)

type QueryFilter struct {
	NodeID string
	Since  int64
}

type Storage interface {
	Save(ctx context.Context, entry *proto.LogEntry) error
	Query(ctx context.Context, filter QueryFilter) ([]*proto.LogEntry, error)
	Delete(ctx context.Context, ids []string) error
	Close() error
}
