// Command node runs a single multi-region node.Node as a standalone
// process, configured from a JSON file. Depending on config it acts as the
// Trung tam (root), a Chi nhanh (branch), or a Leaf — see README.md.
//
// The framework (package node) only moves opaque Envelope data between
// parent and child; it has no notion of "log" or "config" and does not
// persist anything. This service defines those concepts itself: it treats
// upstream Envelopes of kind "log" as agent-submitted log entries, and
// downstream Envelopes of kind "config" as admin-pushed config, and it
// keeps its own local record of both in a BoltDB file it owns
// (examples/node/storage) purely for its own admin API — the framework
// never sees or touches that storage.
//
// It exposes two REST API groups on the same HTTP address:
//
//   - api/v1/agent/...  — agents feeding this node log entries.
//   - api/v1/admin/...  — administration of this specific node (push
//     config down its subtree, inspect its locally-stored logs, check its
//     status, list connected children).
package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/anbebong/multi-region/auth"
	"github.com/anbebong/multi-region/examples/node/storage"
	"github.com/anbebong/multi-region/node"
	"github.com/anbebong/multi-region/proto"
	"github.com/anbebong/multi-region/resolver"
)

//go:embed dashboard.html
var dashboardHTML []byte

const (
	kindLog    = "log"
	kindConfig = "config"
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

	allowList := newChildAllowList(cfg.AllowedChildIDs)

	opts := []node.Option{
		node.WithID(cfg.ID),
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
		opts = append(opts, node.WithAuthorizeChild(allowList.Authorize))
		log.Printf("restricting children to node-ids: %v (editable via api/v1/admin/allowed-children)", allowList.List())
	} else {
		log.Printf("warning: no \"tls\" section in config — running with insecure gRPC (fine for local trials only)")
	}

	n, err := node.New(opts...)
	if err != nil {
		log.Fatalf("create node: %v", err)
	}

	// This service's own business logic: keep a local record of every log
	// Envelope that passes through this node (received from a child or
	// produced locally via SendUp), and apply+keep a record of every config
	// Envelope pushed down from this node's parent. The framework itself
	// never looks inside these Envelopes — it only delivers them.
	n.OnUpstream(kindLog, func(ctx context.Context, env *proto.Envelope) {
		if err := store.Save(ctx, env); err != nil {
			log.Printf("[node %s] save log envelope id=%s failed: %v", n.ID(), env.Id, err)
		}
	})
	n.OnDownstream(kindConfig, func(env *proto.Envelope) {
		if err := store.Save(context.Background(), env); err != nil {
			log.Printf("[node %s] save config envelope id=%s failed: %v", n.ID(), env.Id, err)
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := n.Start(ctx); err != nil {
		log.Fatalf("start node: %v", err)
	}
	log.Printf("node %q started (listen=%q parent=%q)", cfg.ID, cfg.ListenAddr, cfg.ParentAddr)

	var httpServer *http.Server
	if cfg.HTTPAddr != "" {
		httpServer = startAPIServer(cfg.HTTPAddr, n, store, allowList)
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

// startAPIServer wires up the two REST API groups this node exposes:
//
//   - POST api/v1/agent/logs   — an agent submits a log entry.
//   - POST api/v1/admin/config — push config down this node's subtree.
//   - GET  api/v1/admin/logs   — query this node's locally-stored log entries.
//   - GET  api/v1/admin/status — this node's identity/connectivity summary.
//   - GET  api/v1/admin/children — number of children currently connected.
//   - GET/POST/DELETE api/v1/admin/allowed-children — manage which node-ids
//     may connect as children of this node, without editing config.json or
//     restarting the process.
func startAPIServer(addr string, n *node.Node, store storage.Storage, allowList *childAllowList) *http.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /", handleDashboard)

	mux.HandleFunc("POST /api/v1/agent/logs", handleAgentLog(n))

	mux.HandleFunc("POST /api/v1/admin/config", handleAdminConfig(n))
	mux.HandleFunc("POST /api/v1/admin/config/{child_id}", handleAdminConfigForChild(n))
	mux.HandleFunc("GET /api/v1/admin/logs", handleAdminLogs(store))
	mux.HandleFunc("GET /api/v1/admin/status", handleAdminStatus(n))
	mux.HandleFunc("GET /api/v1/admin/children", handleAdminChildren(n))
	mux.HandleFunc("GET /api/v1/admin/allowed-children", handleGetAllowedChildren(allowList))
	mux.HandleFunc("POST /api/v1/admin/allowed-children", handleApproveChild(allowList))
	mux.HandleFunc("DELETE /api/v1/admin/allowed-children/{node_id}", handleRevokeChild(allowList))

	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("api server error: %v", err)
		}
	}()
	log.Printf("api server listening on %s (api/v1/agent/..., api/v1/admin/...)", addr)
	return srv
}

// handleDashboard serves a small static HTML page that calls this node's
// own api/v1/admin/... endpoints from the browser — a minimal stand-in for
// "the web" in the agent -> service -> web chain.
func handleDashboard(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(dashboardHTML)
}

// handleAgentLog accepts a log entry from an agent that has been configured
// to report to this node (root or branch — the agent doesn't need to know
// which). It saves its own local record, then hands it to the framework's
// SendUp so the framework forwards it toward the root, if this node has a
// parent.
//
//	curl -X POST localhost:8080/api/v1/agent/logs -d '{"payload":"hello"}'
func handleAgentLog(n *node.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Payload string `json:"payload"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, fmt.Sprintf("parse json: %v", err), http.StatusBadRequest)
			return
		}

		log.Printf("[agent] received log from %s: payload=%q", r.RemoteAddr, req.Payload)
		if err := n.SendUp(r.Context(), kindLog, []byte(req.Payload)); err != nil {
			log.Printf("[agent] SendUp failed: %v", err)
			http.Error(w, fmt.Sprintf("send up: %v", err), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		writeJSON(w, map[string]string{"status": "accepted"})
	}
}

// handleAdminConfig pushes config down to this node's directly connected
// children, which the framework recursively forwards further down the
// subtree. A no-op (200 OK) if this node has no children.
//
//	curl -X POST localhost:8080/api/v1/admin/config -d '{"data":"..."}'
func handleAdminConfig(n *node.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Data string `json:"data"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, fmt.Sprintf("parse json: %v", err), http.StatusBadRequest)
			return
		}

		log.Printf("[admin] pushing config to %d connected children", n.ChildrenCount())
		if err := n.SendDown(r.Context(), kindConfig, []byte(req.Data)); err != nil {
			log.Printf("[admin] SendDown failed: %v", err)
			http.Error(w, fmt.Sprintf("send down: %v", err), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}
}

// handleAdminConfigForChild pushes config to exactly one directly connected
// child, identified by its node-id in the URL path. Unlike
// handleAdminConfig (broadcast to all children), this targets a single
// child — e.g. to give one branch a different parameter than its siblings.
//
//	curl -X POST localhost:8080/api/v1/admin/config/branch-1 -d '{"data":"..."}'
func handleAdminConfigForChild(n *node.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		childID := r.PathValue("child_id")
		var req struct {
			Data string `json:"data"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, fmt.Sprintf("parse json: %v", err), http.StatusBadRequest)
			return
		}

		log.Printf("[admin] pushing config to child %q", childID)
		if err := n.SendToChild(childID, kindConfig, []byte(req.Data)); err != nil {
			log.Printf("[admin] SendToChild failed: %v", err)
			http.Error(w, fmt.Sprintf("send to child: %v", err), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}
}

// handleAdminLogs queries this node's own local record of log Envelopes.
// Supports optional "since" (unix seconds) query parameter.
//
//	curl "localhost:8080/api/v1/admin/logs?since=0"
func handleAdminLogs(store storage.Storage) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		filter := storage.QueryFilter{Kind: kindLog}
		if s := r.URL.Query().Get("since"); s != "" {
			since, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				http.Error(w, fmt.Sprintf("parse since: %v", err), http.StatusBadRequest)
				return
			}
			filter.Since = since
		}

		entries, err := store.Query(r.Context(), filter)
		if err != nil {
			log.Printf("[admin] query logs failed: %v", err)
			http.Error(w, fmt.Sprintf("query: %v", err), http.StatusInternalServerError)
			return
		}
		log.Printf("[admin] queried logs: since=%d -> %d entries", filter.Since, len(entries))
		writeJSON(w, entries)
	}
}

// handleAdminStatus reports this node's identity and connectivity summary.
//
//	curl localhost:8080/api/v1/admin/status
func handleAdminStatus(n *node.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Printf("[admin] status requested by %s", r.RemoteAddr)
		writeJSON(w, map[string]any{
			"id":                  n.ID(),
			"listen_addr":         n.ListenAddr(),
			"has_parent":          n.HasParent(),
			"connected_to_parent": n.ConnectedToParent(),
			"children_count":      n.ChildrenCount(),
		})
	}
}

// handleAdminChildren reports how many children are currently connected to
// this node. The transport layer only tracks a count today, not per-child
// identity.
//
//	curl localhost:8080/api/v1/admin/children
func handleAdminChildren(n *node.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]int{"children_count": n.ChildrenCount()})
	}
}

// handleGetAllowedChildren lists the node-ids currently allowed to connect
// as children of this node — the UI for this is the "Phê duyệt con kết
// nối" section in dashboard.html.
//
//	curl localhost:8080/api/v1/admin/allowed-children
func handleGetAllowedChildren(allowList *childAllowList) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string][]string{"allowed_child_ids": allowList.List()})
	}
}

// handleApproveChild adds a node-id to the allow list at runtime — this is
// the "approve" action: a child claiming this node-id (and presenting a
// matching mTLS certificate) will be accepted on its next connection
// attempt, without editing config.json or restarting the process.
//
//	curl -X POST localhost:8080/api/v1/admin/allowed-children -d '{"node_id":"branch-2"}'
func handleApproveChild(allowList *childAllowList) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			NodeID string `json:"node_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, fmt.Sprintf("parse json: %v", err), http.StatusBadRequest)
			return
		}
		if req.NodeID == "" {
			http.Error(w, `"node_id" is required`, http.StatusBadRequest)
			return
		}
		allowList.Add(req.NodeID)
		log.Printf("[admin] approved node-id %q to connect as a child", req.NodeID)
		w.WriteHeader(http.StatusAccepted)
		writeJSON(w, map[string][]string{"allowed_child_ids": allowList.List()})
	}
}

// handleRevokeChild removes a node-id from the allow list at runtime — the
// "reject/revoke" action. It does not disconnect a child that is already
// connected (the approval check only runs once, at connection time); it
// only prevents that node-id from connecting again in the future.
//
//	curl -X DELETE localhost:8080/api/v1/admin/allowed-children/branch-2
func handleRevokeChild(allowList *childAllowList) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		nodeID := r.PathValue("node_id")
		allowList.Remove(nodeID)
		log.Printf("[admin] revoked node-id %q from the allowed list", nodeID)
		w.WriteHeader(http.StatusNoContent)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
