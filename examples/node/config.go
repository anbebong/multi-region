package main

import (
	"encoding/json"
	"fmt"
	"os"
)

// FileConfig is the on-disk JSON shape for running a node.Node as a
// standalone process. It exists only in examples/node — it is not part of the
// library's public API.
type FileConfig struct {
	// ID is this node's identifier.
	ID string `json:"id"`

	// ListenAddr, if set, makes this node accept connections from children
	// (it acts as a parent). Omit for a leaf node with no children.
	ListenAddr string `json:"listen_addr,omitempty"`

	// ParentAddr, if set, makes this node connect up to a parent at that
	// address. Omit for a root node with no parent.
	//
	// Setting both ListenAddr and ParentAddr makes this node a Chi nhanh
	// (both a parent to its children and a child of its own parent).
	// Setting only ListenAddr makes it a Trung tam (root). Setting only
	// ParentAddr makes it a Leaf. There is no separate "role" setting —
	// capability follows directly from which addresses are configured.
	ParentAddr string `json:"parent_addr,omitempty"`

	// StoragePath is the BoltDB file path for this service's own local
	// record of log/config Envelopes (see examples/node/storage) — this is
	// this service's own persistence choice, not something the framework
	// requires or knows about.
	StoragePath string `json:"storage_path"`

	// HTTPAddr, if set, starts a local HTTP server exposing api/v1/agent/...
	// (for agents reporting logs), api/v1/admin/... (for administering this
	// node), and a small HTML dashboard at "/" that calls the admin API.
	HTTPAddr string `json:"http_addr,omitempty"`

	// TLS is optional; when omitted the node runs with insecure gRPC
	// (fine for local trials, NOT for production per the design spec).
	TLS *TLSConfig `json:"tls,omitempty"`
}

type TLSConfig struct {
	CACertPath string `json:"ca_cert_path"`
	CertPath   string `json:"cert_path"`
	KeyPath    string `json:"key_path"`
}

func loadConfig(path string) (*FileConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	var cfg FileConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}
	if cfg.ID == "" {
		return nil, fmt.Errorf("config: \"id\" is required")
	}
	if cfg.StoragePath == "" {
		return nil, fmt.Errorf("config: \"storage_path\" is required")
	}
	if cfg.ListenAddr == "" && cfg.ParentAddr == "" {
		return nil, fmt.Errorf("config: at least one of \"listen_addr\" or \"parent_addr\" is required")
	}
	return &cfg, nil
}
