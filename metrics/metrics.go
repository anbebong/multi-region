// Package metrics defines the Prometheus metrics the framework core
// (transport, node) records about its own transport mechanism: connection
// counts, Envelope throughput, forwarding latency, and rejections/drops.
// It knows nothing about "log" or "config" — only about the opaque
// Envelopes and connections the framework moves, exactly like the rest of
// the core.
//
// A service using this framework mounts Handler() on its own HTTP server
// (e.g. at "/metrics") to expose these to Prometheus. The framework never
// opens an HTTP server itself.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const namespace = "multiregion"

var (
	// EnvelopesSent counts Envelopes successfully handed to the transport
	// layer for sending, labeled by direction ("upstream"/"downstream").
	EnvelopesSent = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "envelopes_sent_total",
		Help:      "Total Envelopes successfully sent, by direction.",
	}, []string{"direction"})

	// EnvelopesReceived counts Envelopes received from the network, labeled
	// by direction.
	EnvelopesReceived = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "envelopes_received_total",
		Help:      "Total Envelopes received, by direction.",
	}, []string{"direction"})

	// EnvelopesDropped counts Envelopes the transport layer could not
	// deliver and discarded (e.g. a child's downstream send buffer was
	// full). Labeled by direction and reason.
	EnvelopesDropped = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "envelopes_dropped_total",
		Help:      "Total Envelopes dropped by the transport layer, by direction and reason.",
	}, []string{"direction", "reason"})

	// ChildConnections tracks the current number of connected children,
	// labeled by which RPC ("upstream"/"downstream").
	ChildConnections = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "child_connections",
		Help:      "Current number of connected children, by RPC.",
	}, []string{"rpc"})

	// ChildRejections counts connection attempts rejected by
	// AuthorizeChild.
	ChildRejections = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "child_rejections_total",
		Help:      "Total child connection attempts rejected by AuthorizeChild.",
	})

	// ForwardLatencySeconds measures how long SendUpstream/forwarding calls
	// take, labeled by outcome ("success"/"failure").
	ForwardLatencySeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "forward_latency_seconds",
		Help:      "Latency of forwarding an Envelope to the parent, by outcome.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"outcome"})
)

// Handler returns the HTTP handler a service mounts (e.g. at "/metrics")
// to expose these metrics to Prometheus. The framework never starts an
// HTTP server itself — this is the one integration point a service wires
// into whatever server it already runs.
func Handler() http.Handler {
	return promhttp.Handler()
}
