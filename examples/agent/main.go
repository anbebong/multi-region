// Command agent simulates a PC agent that periodically reports log entries
// to whichever node.Node-backed service (root/Trung tam or branch/Chi
// nhanh — the agent doesn't need to know) it has been configured to talk
// to, via that service's POST /api/v1/agent/logs REST endpoint.
//
// It exists to complete the chain described in the framework's design:
//
//	agent (PC) --REST--> service (node) --REST--> web (admin)
//
// The agent itself has no dependency on the node/transport/proto packages
// at all — it only speaks plain HTTP/JSON, exactly like a real third-party
// agent implementation would.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// FileConfig is the on-disk JSON shape for running the example agent.
type FileConfig struct {
	// ServiceAddr is the base URL of the node service this agent reports
	// to, e.g. "http://127.0.0.1:8081" (a branch) or
	// "http://127.0.0.1:8080" (root) — any node with an http_addr works.
	ServiceAddr string `json:"service_addr"`

	// IntervalSeconds is how often the agent sends a log entry. Defaults
	// to 5 seconds if unset or zero.
	IntervalSeconds int `json:"interval_seconds,omitempty"`

	// Payload is the message sent on each tick. Defaults to a generic
	// message that includes a counter if unset.
	Payload string `json:"payload,omitempty"`
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "usage: %s <config.json>\n", os.Args[0])
		os.Exit(1)
	}

	cfg, err := loadConfig(os.Args[1])
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	interval := time.Duration(cfg.IntervalSeconds) * time.Second
	if interval <= 0 {
		interval = 5 * time.Second
	}

	url := cfg.ServiceAddr + "/api/v1/agent/logs"
	log.Printf("[agent] reporting to %s every %s", url, interval)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	client := &http.Client{Timeout: 5 * time.Second}
	n := 0
	for {
		n++
		payload := cfg.Payload
		if payload == "" {
			payload = fmt.Sprintf("agent tick #%d", n)
		}
		if err := sendLog(client, url, payload); err != nil {
			log.Printf("[agent] tick #%d failed: %v", n, err)
		} else {
			log.Printf("[agent] tick #%d sent: payload=%q -> accepted", n, payload)
		}

		select {
		case <-sigCh:
			log.Printf("[agent] shutting down")
			return
		case <-ticker.C:
		}
	}
}

func sendLog(client *http.Client, url, payload string) error {
	body, err := json.Marshal(map[string]string{"payload": payload})
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	resp, err := client.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("post: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("unexpected status %s", resp.Status)
	}
	return nil
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
	if cfg.ServiceAddr == "" {
		return nil, fmt.Errorf("config: \"service_addr\" is required")
	}
	return &cfg, nil
}
