package main

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

// childAllowList is this service's own dynamic approval policy: a
// thread-safe set of node-ids allowed to connect as children, editable at
// runtime via the admin API/dashboard (no config file edit + restart
// needed). The framework (transport.AuthorizeChild) has no notion of this
// list — it only calls Authorize at connection time and rejects the
// connection if it returns an error.
type childAllowList struct {
	mu      sync.RWMutex
	allowed map[string]bool
}

func newChildAllowList(initial []string) *childAllowList {
	l := &childAllowList{allowed: make(map[string]bool, len(initial))}
	for _, id := range initial {
		l.allowed[id] = true
	}
	return l
}

func (l *childAllowList) Add(nodeID string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.allowed[nodeID] = true
}

func (l *childAllowList) Remove(nodeID string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.allowed, nodeID)
}

func (l *childAllowList) List() []string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	ids := make([]string, 0, len(l.allowed))
	for id := range l.allowed {
		ids = append(ids, id)
	}
	return ids
}

func (l *childAllowList) contains(nodeID string) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.allowed[nodeID]
}

// Authorize builds a node.WithAuthorizeChild hook backed by this allow
// list. It rejects any child whose claimed node-id is not currently in the
// list, and (as a stronger check) also requires that node-id to match the
// CommonName on the child's mTLS client certificate — otherwise a rogue
// client could claim someone else's node-id in the "node-id" metadata
// header.
func (l *childAllowList) Authorize(ctx context.Context, nodeID string) error {
	if nodeID == "" {
		return fmt.Errorf("child did not send a node-id")
	}
	if !l.contains(nodeID) {
		return fmt.Errorf("node-id %q is not in the allowed list", nodeID)
	}

	p, ok := peer.FromContext(ctx)
	if !ok {
		return fmt.Errorf("no peer info on connection")
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || len(tlsInfo.State.PeerCertificates) == 0 {
		return fmt.Errorf("no client certificate presented")
	}
	cn := tlsInfo.State.PeerCertificates[0].Subject.CommonName
	if cn != nodeID {
		return fmt.Errorf("claimed node-id %q does not match certificate CommonName %q", nodeID, cn)
	}
	return nil
}
