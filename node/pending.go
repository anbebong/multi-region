package node

import (
	"fmt"
	"sync"
	"time"

	"github.com/anbebong/multi-region/proto"
)

// pendingQueue is an in-memory-only holding area for Envelopes that could
// not be forwarded upstream on the first attempt. It is intentionally not
// durable: if the process exits before an envelope is delivered, it is
// lost. Making delivery survive a restart is the responsibility of whatever
// service sits on top of this framework (e.g. it can re-submit via SendUp
// after restart if it kept its own record of what it sent).
type pendingQueue struct {
	mu    sync.Mutex
	items []*proto.Envelope
}

func (q *pendingQueue) push(env *proto.Envelope) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.items = append(q.items, env)
}

func (q *pendingQueue) drain() []*proto.Envelope {
	q.mu.Lock()
	defer q.mu.Unlock()
	items := q.items
	q.items = nil
	return items
}

func newEnvelope(nodeID, kind string, payload []byte) *proto.Envelope {
	return &proto.Envelope{
		Id:        fmt.Sprintf("%s-%d", nodeID, time.Now().UnixNano()),
		Kind:      kind,
		Payload:   payload,
		Timestamp: time.Now().Unix(),
	}
}
