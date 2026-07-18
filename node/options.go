package node

import (
	"github.com/anbebong/multi-region/auth"
	"github.com/anbebong/multi-region/resolver"
)

type config struct {
	id         string
	listenAddr string
	resolver   resolver.Resolver
	authn      auth.Authenticator
}

type Option func(*config)

func WithID(id string) Option {
	return func(c *config) { c.id = id }
}

// WithListenAddr makes this node accept connections from children. Omit it
// for a leaf node that has no children of its own.
func WithListenAddr(addr string) Option {
	return func(c *config) { c.listenAddr = addr }
}

// WithResolver makes this node connect up to a parent. Omit it for a root
// node that has no parent of its own.
func WithResolver(r resolver.Resolver) Option {
	return func(c *config) { c.resolver = r }
}

func WithAuthenticator(a auth.Authenticator) Option {
	return func(c *config) { c.authn = a }
}
