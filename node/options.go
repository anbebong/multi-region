package node

import (
	"github.com/anbebong/multi-region/auth"
	"github.com/anbebong/multi-region/resolver"
	"github.com/anbebong/multi-region/transport"
)

type config struct {
	id             string
	listenAddr     string
	resolver       resolver.Resolver
	authn          auth.Authenticator
	authorizeChild transport.AuthorizeChild
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

// WithAuthorizeChild installs a hook that runs once for every incoming
// child connection, right after its mTLS handshake succeeds and before it
// is registered to exchange any Envelope data. Returning a non-nil error
// from authorize rejects the connection.
//
// The framework only provides the hook and the point in the connection
// lifecycle where it runs — it has no opinion on what "authorized" means.
// A typical implementation pulls the child's client certificate out of ctx
// (via peer.FromContext(ctx).AuthInfo.(credentials.TLSInfo)) and checks its
// CommonName/SAN against the service's own allow-list, database, etc.
//
// Only meaningful on a node with WithListenAddr; ignored otherwise.
func WithAuthorizeChild(authorize transport.AuthorizeChild) Option {
	return func(c *config) { c.authorizeChild = authorize }
}
