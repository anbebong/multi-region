package resolver

import "errors"

type Resolver interface {
	ParentAddr() (string, error)
}

type StaticResolver struct {
	addr string
}

func NewStaticResolver(addr string) *StaticResolver {
	return &StaticResolver{addr: addr}
}

func (r *StaticResolver) ParentAddr() (string, error) {
	if r.addr == "" {
		return "", errors.New("resolver: no parent address configured (this node is a root)")
	}
	return r.addr, nil
}
