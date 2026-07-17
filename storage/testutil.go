package storage

import (
	"path/filepath"
	"testing"
)

// MustNewBoltStorage creates a BoltStorage backed by a temp file scoped to
// the test and fails the test immediately on error.
func MustNewBoltStorage(t *testing.T) *BoltStorage {
	t.Helper()
	s, err := NewBoltStorage(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("NewBoltStorage: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}
