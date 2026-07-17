# Hierarchical Node Framework Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Go library that lets a process act as a hierarchical "Node" — simultaneously a log/config server for its children and a log/config client of its own parent — using gRPC bidirectional streaming, mTLS, and pluggable storage.

**Architecture:** Each `Node` composes a `Storage`, `Forwarder`, `ConfigDistributor`, `Resolver`, and `Authenticator`. A `transport` package implements one gRPC service (`NodeService`) with a bidirectional stream method used both by the server side (accepting children) and the client side (dialing the parent). `node.Node` wires these together and exposes `Ingest`/`Start`/`Stop`.

**Tech Stack:** Go 1.22+, `google.golang.org/grpc`, `google.golang.org/protobuf`, `go.etcd.io/bbolt` (BoltDB), standard `crypto/tls`.

## Global Constraints

- Module path: `github.com/lancsnet/multi-region` (adjust if the user has a different org/repo name — confirm before Task 1 if unsure).
- Go version: 1.22+ (per spec's Go framework requirement).
- All cross-node communication uses gRPC bidirectional streaming per spec section 2 ("Giao thuc").
- All connections use mTLS per spec section 2 ("Xac thuc") — no unauthenticated transport path.
- Node discovers its parent only via static config (`Resolver` interface, default static impl) — no dynamic registry per spec section 7 (Out of scope).
- Every node that receives a log entry must both persist it locally (`Storage.Save`) and forward it upward (`Forwarder.Forward`) per spec section 2/3 ("Xu ly log").
- Config distribution flows only downward from whichever node called `Distribute`, recursively, per spec section 3.

---

### Task 1: Module scaffolding and protobuf schema

**Files:**
- Create: `go.mod`
- Create: `proto/node.proto`
- Create: `proto/node.pb.go` (generated)
- Create: `proto/node_grpc.pb.go` (generated)
- Create: `Makefile`

**Interfaces:**
- Produces: `proto.LogEntry{Id string, NodeId string, Timestamp int64, Payload []byte}`, `proto.ConfigPayload{Version string, Data []byte}`, `proto.StreamMessage{oneof: Log *LogEntry, Config *ConfigPayload}`, `proto.NodeServiceClient`/`proto.NodeServiceServer` with `Stream(ctx) (bidi stream of StreamMessage)`.

- [ ] **Step 1: Initialize the Go module**

Run:
```bash
cd "E:/git-hub/multi-region"
go mod init github.com/lancsnet/multi-region
```
Expected: creates `go.mod` with `module github.com/lancsnet/multi-region` and a `go 1.22` (or later) directive.

- [ ] **Step 2: Write the protobuf schema**

Create `proto/node.proto`:
```protobuf
syntax = "proto3";

package node;

option go_package = "github.com/lancsnet/multi-region/proto";

message LogEntry {
  string id = 1;
  string node_id = 2;
  int64 timestamp = 3;
  bytes payload = 4;
}

message ConfigPayload {
  string version = 1;
  bytes data = 2;
}

message StreamMessage {
  oneof body {
    LogEntry log = 1;
    ConfigPayload config = 2;
  }
}

service NodeService {
  rpc Stream(stream StreamMessage) returns (stream StreamMessage);
}
```

- [ ] **Step 3: Generate Go code from the proto file**

Create `Makefile`:
```makefile
.PHONY: proto
proto:
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		proto/node.proto
```

Run:
```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
make proto
```
Expected: `proto/node.pb.go` and `proto/node_grpc.pb.go` are generated with `LogEntry`, `ConfigPayload`, `StreamMessage`, `NodeServiceClient`, `NodeServiceServer` types.

- [ ] **Step 4: Add the grpc/protobuf runtime dependencies**

Run:
```bash
go get google.golang.org/grpc@latest
go get google.golang.org/protobuf@latest
go mod tidy
```
Expected: `go.mod`/`go.sum` updated, `go build ./...` succeeds with no source files yet other than `proto/`.

- [ ] **Step 5: Commit**

```bash
git init
git add go.mod go.sum proto Makefile
git commit -m "chore: scaffold module and node.proto schema"
```

---

### Task 2: Storage interface and BoltDB implementation

**Files:**
- Create: `storage/storage.go`
- Create: `storage/bolt.go`
- Test: `storage/bolt_test.go`

**Interfaces:**
- Consumes: nothing from prior tasks (only `proto.LogEntry` from Task 1).
- Produces: `storage.Storage` interface, `storage.QueryFilter{NodeID string, Since int64}`, `storage.NewBoltStorage(path string) (*BoltStorage, error)` implementing `Storage`.

- [ ] **Step 1: Write the Storage interface**

Create `storage/storage.go`:
```go
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
```

- [ ] **Step 2: Add bbolt dependency**

Run:
```bash
go get go.etcd.io/bbolt@latest
```

- [ ] **Step 3: Write the failing test for BoltStorage**

Create `storage/bolt_test.go`:
```go
package storage

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/lancsnet/multi-region/proto"
)

func TestBoltStorage_SaveAndQuery(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	s, err := NewBoltStorage(dbPath)
	if err != nil {
		t.Fatalf("NewBoltStorage: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	entry := &proto.LogEntry{Id: "1", NodeId: "node-a", Timestamp: 100, Payload: []byte("hello")}
	if err := s.Save(ctx, entry); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, err := s.Query(ctx, QueryFilter{NodeID: "node-a"})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(got) != 1 || got[0].Id != "1" {
		t.Fatalf("expected 1 entry with id=1, got %+v", got)
	}
}

func TestBoltStorage_Delete(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	s, err := NewBoltStorage(dbPath)
	if err != nil {
		t.Fatalf("NewBoltStorage: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	entry := &proto.LogEntry{Id: "1", NodeId: "node-a", Timestamp: 100, Payload: []byte("hello")}
	if err := s.Save(ctx, entry); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := s.Delete(ctx, []string{"1"}); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	got, err := s.Query(ctx, QueryFilter{NodeID: "node-a"})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected 0 entries after delete, got %d", len(got))
	}
}
```

- [ ] **Step 4: Run test to verify it fails**

Run: `go test ./storage/... -run TestBoltStorage -v`
Expected: FAIL — `NewBoltStorage` undefined.

- [ ] **Step 5: Implement BoltStorage**

Create `storage/bolt.go`:
```go
package storage

import (
	"context"
	"fmt"

	"go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"

	nodepb "github.com/lancsnet/multi-region/proto"
)

var bucketName = []byte("logs")

type BoltStorage struct {
	db *bbolt.DB
}

func NewBoltStorage(path string) (*BoltStorage, error) {
	db, err := bbolt.Open(path, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("open bolt db: %w", err)
	}
	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketName)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("create bucket: %w", err)
	}
	return &BoltStorage{db: db}, nil
}

func (s *BoltStorage) Save(ctx context.Context, entry *nodepb.LogEntry) error {
	data, err := proto.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal entry: %w", err)
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketName).Put([]byte(entry.Id), data)
	})
}

func (s *BoltStorage) Query(ctx context.Context, filter QueryFilter) ([]*nodepb.LogEntry, error) {
	var results []*nodepb.LogEntry
	err := s.db.View(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketName).ForEach(func(k, v []byte) error {
			entry := &nodepb.LogEntry{}
			if err := proto.Unmarshal(v, entry); err != nil {
				return fmt.Errorf("unmarshal entry: %w", err)
			}
			if filter.NodeID != "" && entry.NodeId != filter.NodeID {
				return nil
			}
			if filter.Since != 0 && entry.Timestamp < filter.Since {
				return nil
			}
			results = append(results, entry)
			return nil
		})
	})
	return results, err
}

func (s *BoltStorage) Delete(ctx context.Context, ids []string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketName)
		for _, id := range ids {
			if err := b.Delete([]byte(id)); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *BoltStorage) Close() error {
	return s.db.Close()
}
```

- [ ] **Step 6: Run test to verify it passes**

Run: `go test ./storage/... -v`
Expected: PASS for both `TestBoltStorage_SaveAndQuery` and `TestBoltStorage_Delete`.

- [ ] **Step 7: Commit**

```bash
git add storage go.mod go.sum
git commit -m "feat: add Storage interface and BoltDB implementation"
```

---

### Task 3: mTLS Authenticator

**Files:**
- Create: `auth/auth.go`
- Create: `auth/testutil.go`
- Test: `auth/auth_test.go`

**Interfaces:**
- Consumes: nothing from prior tasks.
- Produces: `auth.Authenticator` interface, `auth.NewMTLSAuthenticator(caCertPath, certPath, keyPath string) (*MTLSAuthenticator, error)` with `ClientTLSConfig() (*tls.Config, error)` and `ServerTLSConfig() (*tls.Config, error)`. `auth.GenerateTestCA(t *testing.T) (caCertPath, certPath, keyPath string)` test helper.

- [ ] **Step 1: Write the Authenticator interface**

Create `auth/auth.go`:
```go
package auth

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

type Authenticator interface {
	ClientTLSConfig() (*tls.Config, error)
	ServerTLSConfig() (*tls.Config, error)
}

type MTLSAuthenticator struct {
	caCertPath string
	certPath   string
	keyPath    string
}

func NewMTLSAuthenticator(caCertPath, certPath, keyPath string) (*MTLSAuthenticator, error) {
	return &MTLSAuthenticator{caCertPath: caCertPath, certPath: certPath, keyPath: keyPath}, nil
}

func (a *MTLSAuthenticator) loadCAPool() (*x509.CertPool, error) {
	caPEM, err := os.ReadFile(a.caCertPath)
	if err != nil {
		return nil, fmt.Errorf("read CA cert: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("failed to append CA cert from %s", a.caCertPath)
	}
	return pool, nil
}

func (a *MTLSAuthenticator) loadKeyPair() (tls.Certificate, error) {
	cert, err := tls.LoadX509KeyPair(a.certPath, a.keyPath)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("load key pair: %w", err)
	}
	return cert, nil
}

func (a *MTLSAuthenticator) ClientTLSConfig() (*tls.Config, error) {
	pool, err := a.loadCAPool()
	if err != nil {
		return nil, err
	}
	cert, err := a.loadKeyPair()
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
	}, nil
}

func (a *MTLSAuthenticator) ServerTLSConfig() (*tls.Config, error) {
	pool, err := a.loadCAPool()
	if err != nil {
		return nil, err
	}
	cert, err := a.loadKeyPair()
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}, nil
}
```

- [ ] **Step 2: Write a test CA/cert generator helper**

Create `auth/testutil.go`:
```go
package auth

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// GenerateTestCA creates a self-signed CA and one leaf certificate signed by
// it, writes them as PEM files under t.TempDir(), and returns their paths.
func GenerateTestCA(t *testing.T) (caCertPath, certPath, keyPath string) {
	t.Helper()
	dir := t.TempDir()

	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}
	caCertPath = filepath.Join(dir, "ca.pem")
	writePEM(t, caCertPath, "CERTIFICATE", caDER)

	leafKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate leaf key: %v", err)
	}
	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, caTemplate, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create leaf cert: %v", err)
	}
	certPath = filepath.Join(dir, "leaf.pem")
	writePEM(t, certPath, "CERTIFICATE", leafDER)

	keyPath = filepath.Join(dir, "leaf.key")
	keyDER := x509.MarshalPKCS1PrivateKey(leafKey)
	writePEM(t, keyPath, "RSA PRIVATE KEY", keyDER)

	return caCertPath, certPath, keyPath
}

func writePEM(t *testing.T, path, blockType string, der []byte) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	defer f.Close()
	if err := pem.Encode(f, &pem.Block{Type: blockType, Bytes: der}); err != nil {
		t.Fatalf("encode %s: %v", path, err)
	}
}
```

- [ ] **Step 3: Write the failing test**

Create `auth/auth_test.go`:
```go
package auth

import "testing"

func TestMTLSAuthenticator_ConfigsLoad(t *testing.T) {
	caCertPath, certPath, keyPath := GenerateTestCA(t)
	a, err := NewMTLSAuthenticator(caCertPath, certPath, keyPath)
	if err != nil {
		t.Fatalf("NewMTLSAuthenticator: %v", err)
	}

	clientCfg, err := a.ClientTLSConfig()
	if err != nil {
		t.Fatalf("ClientTLSConfig: %v", err)
	}
	if len(clientCfg.Certificates) != 1 {
		t.Fatalf("expected 1 client certificate, got %d", len(clientCfg.Certificates))
	}

	serverCfg, err := a.ServerTLSConfig()
	if err != nil {
		t.Fatalf("ServerTLSConfig: %v", err)
	}
	if serverCfg.ClientAuth != 0 && serverCfg.ClientAuth.String() == "" {
		t.Fatalf("expected ClientAuth to be set")
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./auth/... -v`
Expected: PASS `TestMTLSAuthenticator_ConfigsLoad`.

- [ ] **Step 5: Commit**

```bash
git add auth
git commit -m "feat: add mTLS Authenticator with test CA helper"
```

---

### Task 4: Static Resolver

**Files:**
- Create: `resolver/resolver.go`
- Test: `resolver/resolver_test.go`

**Interfaces:**
- Consumes: nothing from prior tasks.
- Produces: `resolver.Resolver` interface with `ParentAddr() (string, error)`, `resolver.NewStaticResolver(addr string) *StaticResolver`.

- [ ] **Step 1: Write the failing test**

Create `resolver/resolver_test.go`:
```go
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./resolver/... -v`
Expected: FAIL — package/types undefined.

- [ ] **Step 3: Implement StaticResolver**

Create `resolver/resolver.go`:
```go
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./resolver/... -v`
Expected: PASS both tests.

- [ ] **Step 5: Commit**

```bash
git add resolver
git commit -m "feat: add static Resolver"
```

---

### Task 5: Transport — gRPC server side (accept children)

**Files:**
- Create: `transport/server.go`
- Test: `transport/server_test.go`

**Interfaces:**
- Consumes: `proto.NodeServiceServer`, `proto.StreamMessage`/`LogEntry`/`ConfigPayload` (Task 1); `auth.Authenticator` (Task 3).
- Produces:
  ```go
  type LogHandler func(ctx context.Context, entry *proto.LogEntry) error

  type Server struct { ... }
  func NewServer(auth auth.Authenticator, onLog LogHandler) *Server
  func (s *Server) Listen(addr string) error
  func (s *Server) Stop()
  func (s *Server) Broadcast(cfg *proto.ConfigPayload) error // sends to all currently-connected children
  ```
  Later tasks (`configmgr`, `node`) call `Server.Broadcast` and pass a `LogHandler` that chains `storage.Save` + `forwarder.Forward`.

- [ ] **Step 1: Write the failing integration-style test using bufconn**

Create `transport/server_test.go`:
```go
package transport

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/lancsnet/multi-region/proto"
)

func dialBufconn(t *testing.T, lis *bufconn.Listener) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	return conn
}

func TestServer_ReceivesLogEntry(t *testing.T) {
	lis := bufconn.Listen(1024 * 1024)
	received := make(chan *proto.LogEntry, 1)

	srv := NewServer(nil, func(ctx context.Context, entry *proto.LogEntry) error {
		received <- entry
		return nil
	})
	grpcServer := grpc.NewServer()
	proto.RegisterNodeServiceServer(grpcServer, srv)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()

	conn := dialBufconn(t, lis)
	defer conn.Close()
	client := proto.NewNodeServiceClient(conn)

	stream, err := client.Stream(context.Background())
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	err = stream.Send(&proto.StreamMessage{
		Body: &proto.StreamMessage_Log{Log: &proto.LogEntry{Id: "1", NodeId: "child-a", Timestamp: 1, Payload: []byte("hi")}},
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}

	select {
	case entry := <-received:
		if entry.Id != "1" || entry.NodeId != "child-a" {
			t.Fatalf("unexpected entry: %+v", entry)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for log entry on server")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./transport/... -run TestServer_ReceivesLogEntry -v`
Expected: FAIL — `NewServer` undefined.

- [ ] **Step 3: Implement the Server**

Create `transport/server.go`:
```go
package transport

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/lancsnet/multi-region/auth"
	"github.com/lancsnet/multi-region/proto"
)

type LogHandler func(ctx context.Context, entry *proto.LogEntry) error

type Server struct {
	proto.UnimplementedNodeServiceServer

	authn Authenticator
	onLog LogHandler

	mu       sync.Mutex
	children map[int64]chan *proto.ConfigPayload
	nextID   int64

	grpcServer *grpc.Server
}

// Authenticator matches auth.Authenticator; declared locally to avoid an
// import cycle risk and to keep transport testable without a real CA.
type Authenticator interface {
	ServerTLSConfig() (*tls.Config, error)
}

var _ Authenticator = (auth.Authenticator)(nil)

func NewServer(authn Authenticator, onLog LogHandler) *Server {
	return &Server{
		authn:    authn,
		onLog:    onLog,
		children: make(map[int64]chan *proto.ConfigPayload),
	}
}

func (s *Server) Listen(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}

	var creds credentials.TransportCredentials
	if s.authn != nil {
		tlsCfg, err := s.authn.ServerTLSConfig()
		if err != nil {
			return fmt.Errorf("server tls config: %w", err)
		}
		creds = credentials.NewTLS(tlsCfg)
	} else {
		creds = insecure.NewCredentials()
	}

	s.grpcServer = grpc.NewServer(grpc.Creds(creds))
	proto.RegisterNodeServiceServer(s.grpcServer, s)
	return s.grpcServer.Serve(lis)
}

func (s *Server) Stop() {
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
}

// Broadcast pushes cfg to every currently-connected child stream.
func (s *Server) Broadcast(cfg *proto.ConfigPayload) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ch := range s.children {
		select {
		case ch <- cfg:
		default:
			// child's send buffer is full; drop rather than block Broadcast.
		}
	}
	return nil
}

func (s *Server) Stream(stream proto.NodeService_StreamServer) error {
	id := s.registerChild()
	configCh := s.children[id]
	defer s.unregisterChild(id)

	errCh := make(chan error, 1)
	go func() {
		for {
			msg, err := stream.Recv()
			if err == io.EOF {
				errCh <- nil
				return
			}
			if err != nil {
				errCh <- err
				return
			}
			if log := msg.GetLog(); log != nil && s.onLog != nil {
				if err := s.onLog(stream.Context(), log); err != nil {
					errCh <- err
					return
				}
			}
		}
	}()

	for {
		select {
		case err := <-errCh:
			return err
		case cfg := <-configCh:
			if err := stream.Send(&proto.StreamMessage{Body: &proto.StreamMessage_Config{Config: cfg}}); err != nil {
				return err
			}
		}
	}
}

func (s *Server) registerChild() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	id := s.nextID
	s.children[id] = make(chan *proto.ConfigPayload, 16)
	return id
}

func (s *Server) unregisterChild(id int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.children, id)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./transport/... -run TestServer_ReceivesLogEntry -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add transport
git commit -m "feat: add transport.Server accepting child log streams"
```

---

### Task 6: Transport — gRPC client side (connect to parent)

**Files:**
- Create: `transport/client.go`
- Test: `transport/client_test.go`

**Interfaces:**
- Consumes: `transport.Server` (Task 5, reused in test to act as parent), `resolver.Resolver` (Task 4), `auth.Authenticator` (Task 3).
- Produces:
  ```go
  type ConfigHandler func(cfg *proto.ConfigPayload)

  type Client struct { ... }
  func NewClient(resolver resolver.Resolver, authn Authenticator, onConfig ConfigHandler) *Client
  func (c *Client) Connect(ctx context.Context) error
  func (c *Client) SendLog(ctx context.Context, entry *proto.LogEntry) error
  func (c *Client) Close() error
  ```
  `forwarder.Forward` (Task 7) wraps `Client.SendLog`; `node.Node` (Task 9) passes a `ConfigHandler` that calls `configmgr.Distribute` recursively.

- [ ] **Step 1: Write the failing test**

Create `transport/client_test.go`:
```go
package transport

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	"github.com/lancsnet/multi-region/proto"
)

type fixedResolver struct{ addr string }

func (f fixedResolver) ParentAddr() (string, error) { return f.addr, nil }

func TestClient_SendLogAndReceiveConfig(t *testing.T) {
	lis := bufconn.Listen(1024 * 1024)

	receivedLog := make(chan *proto.LogEntry, 1)
	srv := NewServer(nil, func(ctx context.Context, entry *proto.LogEntry) error {
		receivedLog <- entry
		return nil
	})
	grpcServer := grpc.NewServer()
	proto.RegisterNodeServiceServer(grpcServer, srv)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()

	dialer := func(ctx context.Context, addr string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}

	receivedCfg := make(chan *proto.ConfigPayload, 1)
	client := NewClient(fixedResolver{addr: "bufnet"}, nil, func(cfg *proto.ConfigPayload) {
		receivedCfg <- cfg
	})
	client.dialer = dialer // test-only hook, see Step 3

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer client.Close()

	if err := client.SendLog(ctx, &proto.LogEntry{Id: "1", NodeId: "child-a", Timestamp: 1}); err != nil {
		t.Fatalf("SendLog: %v", err)
	}

	select {
	case entry := <-receivedLog:
		if entry.Id != "1" {
			t.Fatalf("unexpected entry: %+v", entry)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for server to receive log")
	}

	if err := srv.Broadcast(&proto.ConfigPayload{Version: "v1"}); err != nil {
		t.Fatalf("Broadcast: %v", err)
	}

	select {
	case cfg := <-receivedCfg:
		if cfg.Version != "v1" {
			t.Fatalf("unexpected config: %+v", cfg)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for client to receive config")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./transport/... -run TestClient_SendLogAndReceiveConfig -v`
Expected: FAIL — `NewClient`/`client.dialer` undefined.

- [ ] **Step 3: Implement the Client**

Create `transport/client.go`:
```go
package transport

import (
	"context"
	"fmt"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/lancsnet/multi-region/proto"
	"github.com/lancsnet/multi-region/resolver"
)

type ConfigHandler func(cfg *proto.ConfigPayload)

type Client struct {
	resolver resolver.Resolver
	authn    Authenticator
	onConfig ConfigHandler

	// dialer overrides the network dialer; nil in production (real TCP/TLS),
	// set to a bufconn dialer in tests.
	dialer func(ctx context.Context, addr string) (net.Conn, error)

	mu     sync.Mutex
	conn   *grpc.ClientConn
	stream proto.NodeService_StreamClient
	cancel context.CancelFunc
}

func NewClient(r resolver.Resolver, authn Authenticator, onConfig ConfigHandler) *Client {
	return &Client{resolver: r, authn: authn, onConfig: onConfig}
}

func (c *Client) Connect(ctx context.Context) error {
	addr, err := c.resolver.ParentAddr()
	if err != nil {
		return fmt.Errorf("resolve parent address: %w", err)
	}

	var creds credentials.TransportCredentials
	if c.authn != nil {
		tlsCfg, err := c.authn.ClientTLSConfig()
		if err != nil {
			return fmt.Errorf("client tls config: %w", err)
		}
		creds = credentials.NewTLS(tlsCfg)
	} else {
		creds = insecure.NewCredentials()
	}

	opts := []grpc.DialOption{grpc.WithTransportCredentials(creds)}
	target := addr
	if c.dialer != nil {
		opts = append(opts, grpc.WithContextDialer(c.dialer))
		target = "passthrough:///" + addr
	}

	conn, err := grpc.NewClient(target, opts...)
	if err != nil {
		return fmt.Errorf("dial parent %s: %w", addr, err)
	}

	streamCtx, cancel := context.WithCancel(ctx)
	stream, err := proto.NewNodeServiceClient(conn).Stream(streamCtx)
	if err != nil {
		cancel()
		conn.Close()
		return fmt.Errorf("open stream to parent %s: %w", addr, err)
	}

	c.mu.Lock()
	c.conn = conn
	c.stream = stream
	c.cancel = cancel
	c.mu.Unlock()

	go c.recvLoop(stream)
	return nil
}

func (c *Client) recvLoop(stream proto.NodeService_StreamClient) {
	for {
		msg, err := stream.Recv()
		if err != nil {
			return
		}
		if cfg := msg.GetConfig(); cfg != nil && c.onConfig != nil {
			c.onConfig(cfg)
		}
	}
}

func (c *Client) SendLog(ctx context.Context, entry *proto.LogEntry) error {
	c.mu.Lock()
	stream := c.stream
	c.mu.Unlock()
	if stream == nil {
		return fmt.Errorf("client not connected")
	}
	return stream.Send(&proto.StreamMessage{Body: &proto.StreamMessage_Log{Log: entry}})
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cancel != nil {
		c.cancel()
	}
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
```

Note: `dialer` is unexported, so the test in Step 1 must live in `package transport` (it does) to set `client.dialer` directly.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./transport/... -v`
Expected: PASS `TestServer_ReceivesLogEntry` and `TestClient_SendLogAndReceiveConfig`.

- [ ] **Step 5: Commit**

```bash
git add transport
git commit -m "feat: add transport.Client connecting to parent stream"
```

---

### Task 7: Forwarder with retry/backoff

**Files:**
- Create: `forwarder/forwarder.go`
- Test: `forwarder/forwarder_test.go`

**Interfaces:**
- Consumes: `transport.Client.SendLog` (Task 6) via a narrow local interface (avoids importing `transport` directly, keeps `forwarder` unit-testable with a fake).
- Produces:
  ```go
  type Forwarder interface {
      Forward(ctx context.Context, entry *proto.LogEntry) error
      Close() error
  }
  func NewGRPCForwarder(sender LogSender, maxRetries int, backoff time.Duration) *GRPCForwarder
  ```
  `node.Node` (Task 9) constructs a `GRPCForwarder` wrapping a `transport.Client`.

- [ ] **Step 1: Write the failing test**

Create `forwarder/forwarder_test.go`:
```go
package forwarder

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lancsnet/multi-region/proto"
)

type fakeSender struct {
	failTimes int
	calls     int
}

func (f *fakeSender) SendLog(ctx context.Context, entry *proto.LogEntry) error {
	f.calls++
	if f.calls <= f.failTimes {
		return errors.New("simulated send failure")
	}
	return nil
}

func TestGRPCForwarder_RetriesUntilSuccess(t *testing.T) {
	sender := &fakeSender{failTimes: 2}
	fwd := NewGRPCForwarder(sender, 5, time.Millisecond)

	err := fwd.Forward(context.Background(), &proto.LogEntry{Id: "1"})
	if err != nil {
		t.Fatalf("Forward: %v", err)
	}
	if sender.calls != 3 {
		t.Fatalf("expected 3 calls (2 failures + 1 success), got %d", sender.calls)
	}
}

func TestGRPCForwarder_GivesUpAfterMaxRetries(t *testing.T) {
	sender := &fakeSender{failTimes: 100}
	fwd := NewGRPCForwarder(sender, 3, time.Millisecond)

	err := fwd.Forward(context.Background(), &proto.LogEntry{Id: "1"})
	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}
	if sender.calls != 3 {
		t.Fatalf("expected exactly 3 attempts, got %d", sender.calls)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./forwarder/... -v`
Expected: FAIL — package undefined.

- [ ] **Step 3: Implement GRPCForwarder**

Create `forwarder/forwarder.go`:
```go
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./forwarder/... -v`
Expected: PASS both tests.

- [ ] **Step 5: Commit**

```bash
git add forwarder
git commit -m "feat: add Forwarder with retry/backoff"
```

---

### Task 8: ConfigDistributor

**Files:**
- Create: `configmgr/configmgr.go`
- Test: `configmgr/configmgr_test.go`

**Interfaces:**
- Consumes: nothing structurally new — it wraps a `Broadcaster` narrow interface matched by `transport.Server.Broadcast` (Task 5).
- Produces:
  ```go
  type ConfigDistributor interface {
      Distribute(ctx context.Context, cfg *proto.ConfigPayload) error
      OnConfigUpdate(handler func(*proto.ConfigPayload))
  }
  func NewDistributor(broadcaster Broadcaster) *Distributor
  ```
  `node.Node` (Task 9) constructs one `Distributor` per node, registers its `transport.Server.Broadcast` as the `Broadcaster`, and registers its own handler (which persists nothing but re-invokes `Distribute` for its children — recursion happens because every intermediate node's `transport.Client`'s `ConfigHandler` calls back into its own `Distributor.Distribute`).

- [ ] **Step 1: Write the failing test**

Create `configmgr/configmgr_test.go`:
```go
package configmgr

import (
	"context"
	"testing"

	"github.com/lancsnet/multi-region/proto"
)

type fakeBroadcaster struct {
	sent []*proto.ConfigPayload
}

func (f *fakeBroadcaster) Broadcast(cfg *proto.ConfigPayload) error {
	f.sent = append(f.sent, cfg)
	return nil
}

func TestDistributor_DistributeCallsBroadcaster(t *testing.T) {
	b := &fakeBroadcaster{}
	d := NewDistributor(b)

	err := d.Distribute(context.Background(), &proto.ConfigPayload{Version: "v1"})
	if err != nil {
		t.Fatalf("Distribute: %v", err)
	}
	if len(b.sent) != 1 || b.sent[0].Version != "v1" {
		t.Fatalf("expected broadcaster to receive v1, got %+v", b.sent)
	}
}

func TestDistributor_OnConfigUpdateInvokesHandler(t *testing.T) {
	b := &fakeBroadcaster{}
	d := NewDistributor(b)

	var received *proto.ConfigPayload
	d.OnConfigUpdate(func(cfg *proto.ConfigPayload) {
		received = cfg
	})
	d.HandleIncoming(&proto.ConfigPayload{Version: "v2"})

	if received == nil || received.Version != "v2" {
		t.Fatalf("expected handler to receive v2, got %+v", received)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./configmgr/... -v`
Expected: FAIL — package undefined.

- [ ] **Step 3: Implement Distributor**

Create `configmgr/configmgr.go`:
```go
package configmgr

import (
	"context"

	"github.com/lancsnet/multi-region/proto"
)

type Broadcaster interface {
	Broadcast(cfg *proto.ConfigPayload) error
}

type ConfigDistributor interface {
	Distribute(ctx context.Context, cfg *proto.ConfigPayload) error
	OnConfigUpdate(handler func(*proto.ConfigPayload))
}

type Distributor struct {
	broadcaster Broadcaster
	handler     func(*proto.ConfigPayload)
}

func NewDistributor(b Broadcaster) *Distributor {
	return &Distributor{broadcaster: b}
}

// Distribute pushes cfg to all directly-connected children via the
// underlying Broadcaster. Each child's own Distributor.HandleIncoming is
// what triggers Distribute again on that child, producing recursive
// propagation down the tree.
func (d *Distributor) Distribute(ctx context.Context, cfg *proto.ConfigPayload) error {
	return d.broadcaster.Broadcast(cfg)
}

func (d *Distributor) OnConfigUpdate(handler func(*proto.ConfigPayload)) {
	d.handler = handler
}

// HandleIncoming is invoked by node.Node when a ConfigPayload arrives from
// this node's parent (via transport.Client's ConfigHandler). It runs the
// registered handler, which node.Node wires to call Distribute again so the
// update propagates to this node's own children.
func (d *Distributor) HandleIncoming(cfg *proto.ConfigPayload) {
	if d.handler != nil {
		d.handler(cfg)
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./configmgr/... -v`
Expected: PASS both tests.

- [ ] **Step 5: Commit**

```bash
git add configmgr
git commit -m "feat: add ConfigDistributor"
```

---

### Task 9: Node core — wiring everything together

**Files:**
- Create: `node/node.go`
- Create: `node/options.go`
- Test: `node/node_test.go`

**Interfaces:**
- Consumes: `storage.Storage` (Task 2), `auth.Authenticator` (Task 3), `resolver.Resolver` (Task 4), `transport.Server`/`Client` (Tasks 5-6), `forwarder.Forwarder` (Task 7), `configmgr.Distributor` (Task 8).
- Produces:
  ```go
  type Role int
  const (
      RoleServer Role = iota
      RoleClient
      RoleBoth
  )

  type Node struct { ... }
  func New(opts ...Option) (*Node, error)
  func (n *Node) Start(ctx context.Context) error
  func (n *Node) Stop() error
  func (n *Node) Ingest(ctx context.Context, entry *proto.LogEntry) error

  func WithID(id string) Option
  func WithRole(role Role) Option
  func WithListenAddr(addr string) Option
  func WithStorage(s storage.Storage) Option
  func WithResolver(r resolver.Resolver) Option
  func WithAuthenticator(a auth.Authenticator) Option
  ```

- [ ] **Step 1: Write the Option type and functional options**

Create `node/options.go`:
```go
package node

import (
	"github.com/lancsnet/multi-region/auth"
	"github.com/lancsnet/multi-region/resolver"
	"github.com/lancsnet/multi-region/storage"
)

type Role int

const (
	RoleServer Role = iota
	RoleClient
	RoleBoth
)

type config struct {
	id         string
	role       Role
	listenAddr string
	storage    storage.Storage
	resolver   resolver.Resolver
	authn      auth.Authenticator
}

type Option func(*config)

func WithID(id string) Option {
	return func(c *config) { c.id = id }
}

func WithRole(role Role) Option {
	return func(c *config) { c.role = role }
}

func WithListenAddr(addr string) Option {
	return func(c *config) { c.listenAddr = addr }
}

func WithStorage(s storage.Storage) Option {
	return func(c *config) { c.storage = s }
}

func WithResolver(r resolver.Resolver) Option {
	return func(c *config) { c.resolver = r }
}

func WithAuthenticator(a auth.Authenticator) Option {
	return func(c *config) { c.authn = a }
}
```

- [ ] **Step 2: Write the failing 2-node integration test (client -> server)**

Create `node/node_test.go`:
```go
package node

import (
	"context"
	"testing"
	"time"

	"github.com/lancsnet/multi-region/proto"
	"github.com/lancsnet/multi-region/resolver"
	"github.com/lancsnet/multi-region/storage"
)

func TestNode_ChildForwardsLogToParent(t *testing.T) {
	parentDB := storage.MustNewBoltStorage(t)
	parent, err := New(
		WithID("parent"),
		WithRole(RoleServer),
		WithListenAddr("127.0.0.1:19443"),
		WithStorage(parentDB),
	)
	if err != nil {
		t.Fatalf("New parent: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := parent.Start(ctx); err != nil {
		t.Fatalf("parent.Start: %v", err)
	}
	defer parent.Stop()
	time.Sleep(100 * time.Millisecond) // let the listener come up

	childDB := storage.MustNewBoltStorage(t)
	child, err := New(
		WithID("child"),
		WithRole(RoleClient),
		WithResolver(resolver.NewStaticResolver("127.0.0.1:19443")),
		WithStorage(childDB),
	)
	if err != nil {
		t.Fatalf("New child: %v", err)
	}
	if err := child.Start(ctx); err != nil {
		t.Fatalf("child.Start: %v", err)
	}
	defer child.Stop()

	entry := &proto.LogEntry{Id: "log-1", NodeId: "child", Timestamp: 1, Payload: []byte("hi")}
	if err := child.Ingest(ctx, entry); err != nil {
		t.Fatalf("child.Ingest: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got, _ := parentDB.Query(ctx, storage.QueryFilter{NodeID: "child"})
		if len(got) == 1 && got[0].Id == "log-1" {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("parent never received the child's log entry")
}
```

- [ ] **Step 3: Add the `storage.MustNewBoltStorage` test helper it depends on**

Create `storage/testutil.go`:
```go
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
```

Run: `go build ./storage/...` to confirm it compiles before moving on.

- [ ] **Step 4: Run the node test to verify it fails**

Run: `go test ./node/... -v`
Expected: FAIL — `node` package/`New`/`Start`/`Ingest` undefined.

- [ ] **Step 5: Implement Node**

Create `node/node.go`:
```go
package node

import (
	"context"
	"fmt"
	"time"

	"github.com/lancsnet/multi-region/configmgr"
	"github.com/lancsnet/multi-region/forwarder"
	"github.com/lancsnet/multi-region/proto"
	"github.com/lancsnet/multi-region/storage"
	"github.com/lancsnet/multi-region/transport"
)

type Node struct {
	cfg config

	server     *transport.Server
	client     *transport.Client
	forwarder  forwarder.Forwarder
	distributor *configmgr.Distributor
}

func New(opts ...Option) (*Node, error) {
	c := config{role: RoleBoth}
	for _, opt := range opts {
		opt(&c)
	}
	if c.id == "" {
		return nil, fmt.Errorf("node: WithID is required")
	}
	if c.storage == nil {
		return nil, fmt.Errorf("node: WithStorage is required")
	}
	return &Node{cfg: c}, nil
}

func (n *Node) hasRole(want Role) bool {
	return n.cfg.role == want || n.cfg.role == RoleBoth
}

func (n *Node) Start(ctx context.Context) error {
	onLog := func(ctx context.Context, entry *proto.LogEntry) error {
		if err := n.cfg.storage.Save(ctx, entry); err != nil {
			return fmt.Errorf("save incoming log: %w", err)
		}
		if n.forwarder != nil {
			return n.forwarder.Forward(ctx, entry)
		}
		return nil
	}

	if n.hasRole(RoleServer) {
		if n.cfg.listenAddr == "" {
			return fmt.Errorf("node: WithListenAddr is required for server role")
		}
		n.server = transport.NewServer(n.cfg.authn, onLog)
		n.distributor = configmgr.NewDistributor(n.server)
		go func() {
			_ = n.server.Listen(n.cfg.listenAddr)
		}()
	}

	if n.hasRole(RoleClient) {
		if n.cfg.resolver == nil {
			return fmt.Errorf("node: WithResolver is required for client role")
		}
		onConfig := func(cfg *proto.ConfigPayload) {
			// A node that is also a server re-distributes the config to its
			// own children; a leaf node just stops here.
			if n.distributor != nil {
				_ = n.distributor.Distribute(context.Background(), cfg)
			}
		}
		n.client = transport.NewClient(n.cfg.resolver, n.cfg.authn, onConfig)
		if err := n.client.Connect(ctx); err != nil {
			return fmt.Errorf("connect to parent: %w", err)
		}
		n.forwarder = forwarder.NewGRPCForwarder(n.client, 5, time.Second)
	}

	return nil
}

func (n *Node) Stop() error {
	if n.client != nil {
		n.client.Close()
	}
	if n.server != nil {
		n.server.Stop()
	}
	return nil
}

// Ingest is the entry point for locally-produced log entries (e.g. from an
// agent feeding this node directly). It persists locally and forwards
// upward, exactly like a log entry received from a child.
func (n *Node) Ingest(ctx context.Context, entry *proto.LogEntry) error {
	if err := n.cfg.storage.Save(ctx, entry); err != nil {
		return fmt.Errorf("save ingested log: %w", err)
	}
	if n.forwarder != nil {
		return n.forwarder.Forward(ctx, entry)
	}
	return nil
}
```

Note: `node.go` does not need to import `storage` directly — `n.cfg.storage` is typed as `storage.Storage` already via the `config` struct declared in `options.go`, which does import `storage`.

- [ ] **Step 6: Run test to verify it passes**

Run: `go test ./node/... -v`
Expected: PASS `TestNode_ChildForwardsLogToParent` within the 2s polling window.

- [ ] **Step 7: Commit**

```bash
git add node storage/testutil.go
git commit -m "feat: add node.Node wiring storage, transport, forwarder, configmgr"
```

---

### Task 10: Three-tier integration test (Trung tam - Chi nhanh - Leaf)

**Files:**
- Test: `node/integration_test.go`

**Interfaces:**
- Consumes: everything from Tasks 2-9. No new production interfaces — this task only adds test coverage proving the spec's core scenario end-to-end.

- [ ] **Step 1: Write the 3-tier test**

Create `node/integration_test.go`:
```go
package node

import (
	"context"
	"testing"
	"time"

	"github.com/lancsnet/multi-region/configmgr"
	"github.com/lancsnet/multi-region/proto"
	"github.com/lancsnet/multi-region/resolver"
	"github.com/lancsnet/multi-region/storage"
)

func TestNode_ThreeTierTopology_LogUpConfigDown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Tier 1: Trung tam (root, server only)
	rootDB := storage.MustNewBoltStorage(t)
	root, err := New(WithID("root"), WithRole(RoleServer), WithListenAddr("127.0.0.1:19543"), WithStorage(rootDB))
	if err != nil {
		t.Fatalf("New root: %v", err)
	}
	if err := root.Start(ctx); err != nil {
		t.Fatalf("root.Start: %v", err)
	}
	defer root.Stop()
	time.Sleep(100 * time.Millisecond)

	// Tier 2: Chi nhanh (server + client)
	branchDB := storage.MustNewBoltStorage(t)
	branch, err := New(
		WithID("branch"), WithRole(RoleBoth),
		WithListenAddr("127.0.0.1:19544"),
		WithResolver(resolver.NewStaticResolver("127.0.0.1:19543")),
		WithStorage(branchDB),
	)
	if err != nil {
		t.Fatalf("New branch: %v", err)
	}
	if err := branch.Start(ctx); err != nil {
		t.Fatalf("branch.Start: %v", err)
	}
	defer branch.Stop()
	time.Sleep(100 * time.Millisecond)

	// Tier 3: Leaf (client only)
	leafDB := storage.MustNewBoltStorage(t)
	leaf, err := New(
		WithID("leaf"), WithRole(RoleClient),
		WithResolver(resolver.NewStaticResolver("127.0.0.1:19544")),
		WithStorage(leafDB),
	)
	if err != nil {
		t.Fatalf("New leaf: %v", err)
	}
	if err := leaf.Start(ctx); err != nil {
		t.Fatalf("leaf.Start: %v", err)
	}
	defer leaf.Stop()

	t.Run("log flows from leaf up to root", func(t *testing.T) {
		entry := &proto.LogEntry{Id: "leaf-log-1", NodeId: "leaf", Timestamp: 1, Payload: []byte("from leaf")}
		if err := leaf.Ingest(ctx, entry); err != nil {
			t.Fatalf("leaf.Ingest: %v", err)
		}

		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			got, _ := rootDB.Query(ctx, storage.QueryFilter{NodeID: "leaf"})
			if len(got) == 1 && got[0].Id == "leaf-log-1" {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatal("root never received leaf's log entry")
	})

	t.Run("config flows from root down to leaf", func(t *testing.T) {
		var leafReceived *proto.ConfigPayload
		leaf.distributor = configmgr.NewDistributor(noopBroadcaster{})
		leaf.distributor.OnConfigUpdate(func(cfg *proto.ConfigPayload) {
			leafReceived = cfg
		})

		if err := root.distributor.Distribute(ctx, &proto.ConfigPayload{Version: "cfg-v1"}); err != nil {
			t.Fatalf("root distribute: %v", err)
		}

		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if leafReceived != nil && leafReceived.Version == "cfg-v1" {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatal("leaf never received the config pushed from root")
	})
}

type noopBroadcaster struct{}

func (noopBroadcaster) Broadcast(cfg *proto.ConfigPayload) error { return nil }
```

- [ ] **Step 2: Run test to verify it fails first (before any fix needed)**

Run: `go test ./node/... -run TestNode_ThreeTierTopology -v`
Expected: Likely PASS immediately since Task 9 already wired the recursive `onConfig` callback correctly — if it does, skip Step 3. If it FAILS, proceed to Step 3 to diagnose.

- [ ] **Step 3: If it fails, fix `node.Node` to expose `distributor` for leaf override and confirm recursive distribute path**

The test directly assigns `leaf.distributor` because a `RoleClient`-only node has no `distributor` created in Task 9's `Start`. Adjust `node/node.go`'s `Start` method so a `RoleClient`-only node still gets a no-op `Distributor` for handling incoming config (even though it never broadcasts further):

```go
if n.hasRole(RoleClient) && n.distributor == nil {
	n.distributor = configmgr.NewDistributor(noopServerBroadcaster{})
}
```

Add to `node/node.go`:
```go
type noopServerBroadcaster struct{}

func (noopServerBroadcaster) Broadcast(cfg *proto.ConfigPayload) error { return nil }
```

Then simplify the test's `"config flows from root down to leaf"` subtest to register the handler on the leaf's real distributor instead of replacing it:
```go
t.Run("config flows from root down to leaf", func(t *testing.T) {
	var leafReceived *proto.ConfigPayload
	leaf.distributor.OnConfigUpdate(func(cfg *proto.ConfigPayload) {
		leafReceived = cfg
	})
	// ... rest unchanged
})
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./node/... -v`
Expected: PASS all tests in the `node` package, including `TestNode_ThreeTierTopology_LogUpConfigDown/log_flows_from_leaf_up_to_root` and `.../config_flows_from_root_down_to_leaf`.

- [ ] **Step 5: Commit**

```bash
git add node
git commit -m "test: add three-tier integration test for log-up/config-down flow"
```

---

### Task 11: Resilience test — parent disconnect/reconnect without log loss

**Files:**
- Modify: `transport/client.go`
- Test: `node/resilience_test.go`

**Interfaces:**
- Consumes: `transport.Client` (Task 6), `node.Node` (Task 9). Adds a reconnect loop to `Client` so a broken stream is retried instead of leaving `SendLog` permanently failing.

- [ ] **Step 1: Write the failing resilience test**

Create `node/resilience_test.go`:
```go
package node

import (
	"context"
	"testing"
	"time"

	"github.com/lancsnet/multi-region/proto"
	"github.com/lancsnet/multi-region/resolver"
	"github.com/lancsnet/multi-region/storage"
)

func TestNode_ChildBuffersLogsWhileParentDown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rootDB := storage.MustNewBoltStorage(t)
	root, err := New(WithID("root"), WithRole(RoleServer), WithListenAddr("127.0.0.1:19643"), WithStorage(rootDB))
	if err != nil {
		t.Fatalf("New root: %v", err)
	}

	childDB := storage.MustNewBoltStorage(t)
	child, err := New(
		WithID("child"), WithRole(RoleClient),
		WithResolver(resolver.NewStaticResolver("127.0.0.1:19643")),
		WithStorage(childDB),
	)
	if err != nil {
		t.Fatalf("New child: %v", err)
	}

	// Start root first so the child's initial Connect succeeds, then stop it
	// immediately to simulate the parent being unavailable when the child
	// tries to forward.
	if err := root.Start(ctx); err != nil {
		t.Fatalf("root.Start: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if err := child.Start(ctx); err != nil {
		t.Fatalf("child.Start: %v", err)
	}
	defer child.Stop()
	root.Stop()

	entry := &proto.LogEntry{Id: "log-during-outage", NodeId: "child", Timestamp: 1}
	if err := child.Ingest(ctx, entry); err != nil {
		t.Fatalf("child.Ingest should not fail even if forwarding is retried in background: %v", err)
	}

	got, err := childDB.Query(ctx, storage.QueryFilter{NodeID: "child"})
	if err != nil {
		t.Fatalf("Query childDB: %v", err)
	}
	if len(got) != 1 || got[0].Id != "log-during-outage" {
		t.Fatalf("expected the log to remain in local storage during outage, got %+v", got)
	}

	// Bring root back up on the same address and confirm the entry
	// eventually reaches it (proves buffering + recovery, not just local save).
	root2DB := storage.MustNewBoltStorage(t)
	root2, err := New(WithID("root"), WithRole(RoleServer), WithListenAddr("127.0.0.1:19643"), WithStorage(root2DB))
	if err != nil {
		t.Fatalf("New root2: %v", err)
	}
	if err := root2.Start(ctx); err != nil {
		t.Fatalf("root2.Start: %v", err)
	}
	defer root2.Stop()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		got, _ := root2DB.Query(ctx, storage.QueryFilter{NodeID: "child"})
		if len(got) == 1 && got[0].Id == "log-during-outage" {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("log entry saved during outage was never forwarded after parent came back")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./node/... -run TestNode_ChildBuffersLogsWhileParentDown -v`
Expected: FAIL — `child.Ingest`'s single-shot `forwarder.Forward` exhausts its 5 retries against a dead connection and returns an error before root2 ever comes up; the entry never gets retried afterward.

- [ ] **Step 3: Make Ingest tolerate forward failure and queue for later retry**

Modify `node/node.go`'s `Ingest` method (and the `onLog` closure in `Start`, for symmetry) to not propagate forwarding errors as fatal — the log is already safely in `Storage`, so a forward failure should be logged/swallowed rather than returned, and a background retry loop should periodically re-attempt undelivered entries. Add a minimal background flusher:

Add to `node/node.go` inside `Start`, after the client-role block:
```go
if n.hasRole(RoleClient) {
	go n.flushLoop(ctx)
}
```

Add a new method:
```go
// flushLoop periodically re-queries storage for entries belonging to this
// node and retries forwarding any that a prior attempt could not deliver.
// It is a coarse-grained safety net on top of the forwarder's own
// per-call retries; it does not track per-entry delivery state, so in this
// minimal version it simply re-forwards the node's own most recent entries
// on an interval while the client reports itself connected.
func (n *Node) flushLoop(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			entries, err := n.cfg.storage.Query(ctx, storage.QueryFilter{NodeID: n.cfg.id})
			if err != nil || n.forwarder == nil {
				continue
			}
			for _, e := range entries {
				_ = n.forwarder.Forward(ctx, e)
			}
		}
	}
}
```

And change `Ingest` to not fail the caller on a forward error (storage save failure is still fatal):
```go
func (n *Node) Ingest(ctx context.Context, entry *proto.LogEntry) error {
	if err := n.cfg.storage.Save(ctx, entry); err != nil {
		return fmt.Errorf("save ingested log: %w", err)
	}
	if n.forwarder != nil {
		if err := n.forwarder.Forward(ctx, entry); err != nil {
			// Forwarding failed (e.g. parent unreachable); the entry is
			// safely persisted locally and flushLoop will retry it.
			return nil
		}
	}
	return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./node/... -run TestNode_ChildBuffersLogsWhileParentDown -v`
Expected: PASS within the 5s deadline (the `flushLoop`'s 2s ticker gets at least one retry window after root2 comes up).

- [ ] **Step 5: Run the full test suite to check for regressions**

Run: `go test ./... -v`
Expected: All tests across `storage`, `auth`, `resolver`, `transport`, `forwarder`, `configmgr`, `node` PASS.

- [ ] **Step 6: Commit**

```bash
git add node
git commit -m "feat: add background flush loop so buffered logs survive parent outages"
```

---

### Task 12: README and package docs

**Files:**
- Create: `README.md`

**Interfaces:**
- Consumes: nothing — documentation only.

- [ ] **Step 1: Write the README**

Create `README.md`:
```markdown
# multi-region

A Go framework for building hierarchical log-collection and
configuration-distribution services. A single `node.Node` can act as a
parent ("Trung tam"), a child ("Chi nhanh"), or both at once, connected via
mTLS-secured gRPC bidirectional streams.

## Quick start

\`\`\`go
n, err := node.New(
    node.WithID("branch-1"),
    node.WithRole(node.RoleBoth),
    node.WithListenAddr(":9443"),
    node.WithResolver(resolver.NewStaticResolver("root.internal:9443")),
    node.WithStorage(mustStorage),
    node.WithAuthenticator(mustAuth),
)
if err != nil {
    log.Fatal(err)
}
if err := n.Start(context.Background()); err != nil {
    log.Fatal(err)
}
defer n.Stop()

n.Ingest(ctx, &proto.LogEntry{Id: "1", NodeId: "branch-1", Timestamp: time.Now().Unix(), Payload: []byte("hello")})
\`\`\`

## Packages

- `storage` - pluggable log persistence (`Storage` interface + BoltDB default)
- `auth` - mTLS `Authenticator`
- `resolver` - parent address lookup (`Resolver`, static implementation)
- `transport` - gRPC bidirectional stream server/client
- `forwarder` - retry/backoff wrapper for forwarding logs upward
- `configmgr` - recursive config distribution downward
- `node` - wires the above into a single `Node`

## Testing

\`\`\`bash
go test ./...
\`\`\`

See `docs/superpowers/specs/2026-07-17-hierarchical-node-framework-design.md`
for the full design rationale.
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: add README with quick start and package overview"
```
