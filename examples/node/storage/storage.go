// Package storage is owned by the examples/node service, not the
// framework core. The framework (node.Node) has no notion of persistence —
// it only moves Envelope data between parent and child. This package is
// how the examples/node service chooses to keep its own local record of
// Envelopes it has sent or received, purely for its own admin API
// (GET /api/v1/admin/logs) and audit purposes.
package storage

import (
	"context"

	"github.com/anbebong/multi-region/proto"
)

type QueryFilter struct {
	Kind  string
	Since int64
}

type Storage interface {
	Save(ctx context.Context, env *proto.Envelope) error
	Query(ctx context.Context, filter QueryFilter) ([]*proto.Envelope, error)
	Delete(ctx context.Context, ids []string) error
	Close() error
}
