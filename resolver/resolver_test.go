package resolver

import "testing"

func TestStaticResolver_ParentAddr(t *testing.T) {
	r := NewStaticResolver("parent.internal:9443")
	addr, err := r.ParentAddr()
	if err != nil {
		t.Fatalf("ParentAddr: %v", err)
	}
	if addr != "parent.internal:9443" {
		t.Fatalf("expected parent.internal:9443, got %s", addr)
	}
}

func TestStaticResolver_EmptyAddrErrors(t *testing.T) {
	r := NewStaticResolver("")
	if _, err := r.ParentAddr(); err == nil {
		t.Fatalf("expected error for empty address")
	}
}
