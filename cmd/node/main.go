// Command node runs a single multi-region node.Node as a standalone
// process, configured from a JSON file. It is a reference binary for
// trying the framework out manually — see README.md and config.example.json.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lancsnet/multi-region/auth"
	"github.com/lancsnet/multi-region/node"
	"github.com/lancsnet/multi-region/proto"
	"github.com/lancsnet/multi-region/resolver"
	"github.com/lancsnet/multi-region/storage"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "usage: %s <config.json>\n", os.Args[0])
		os.Exit(1)
	}

	cfg, err := loadConfig(os.Args[1])
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	store, err := storage.NewBoltStorage(cfg.StoragePath)
	if err != nil {
		log.Fatalf("open storage: %v", err)
	}
	defer store.Close()

	opts := []node.Option{
		node.WithID(cfg.ID),
		node.WithStorage(store),
	}
	if cfg.ListenAddr != "" {
		opts = append(opts, node.WithListenAddr(cfg.ListenAddr))
	}
	if cfg.ParentAddr != "" {
		opts = append(opts, node.WithResolver(resolver.NewStaticResolver(cfg.ParentAddr)))
	}
	if cfg.TLS != nil {
		authn, err := auth.NewMTLSAuthenticator(cfg.TLS.CACertPath, cfg.TLS.CertPath, cfg.TLS.KeyPath)
		if err != nil {
			log.Fatalf("configure mTLS: %v", err)
		}
		opts = append(opts, node.WithAuthenticator(authn))
	} else {
		log.Printf("warning: no \"tls\" section in config — running with insecure gRPC (fine for local trials only)")
	}

	n, err := node.New(opts...)
	if err != nil {
		log.Fatalf("create node: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := n.Start(ctx); err != nil {
		log.Fatalf("start node: %v", err)
	}
	log.Printf("node %q started (listen=%q parent=%q)", cfg.ID, cfg.ListenAddr, cfg.ParentAddr)

	var httpServer *http.Server
	if cfg.HTTPAddr != "" {
		httpServer = startIngestServer(cfg.HTTPAddr, cfg.ID, n)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	log.Printf("shutting down")
	if httpServer != nil {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}
	_ = n.Stop()
}

// startIngestServer exposes POST /ingest {"payload": "..."} so a node
// running as a standalone process can be fed log entries manually, e.g.:
//
//	curl -X POST localhost:8080/ingest -d '{"payload":"hello"}'
func startIngestServer(addr, nodeID string, n *node.Node) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/ingest", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "expected POST", http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, fmt.Sprintf("read body: %v", err), http.StatusBadRequest)
			return
		}
		var req struct {
			Payload string `json:"payload"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, fmt.Sprintf("parse json: %v", err), http.StatusBadRequest)
			return
		}

		entry := &proto.LogEntry{
			Id:        fmt.Sprintf("%s-%d", nodeID, time.Now().UnixNano()),
			NodeId:    nodeID,
			Timestamp: time.Now().Unix(),
			Payload:   []byte(req.Payload),
		}
		if err := n.Ingest(r.Context(), entry); err != nil {
			http.Error(w, fmt.Sprintf("ingest: %v", err), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		fmt.Fprintf(w, "accepted %s\n", entry.Id)
	})

	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("ingest http server error: %v", err)
		}
	}()
	log.Printf("ingest endpoint listening on %s (POST /ingest)", addr)
	return srv
}
