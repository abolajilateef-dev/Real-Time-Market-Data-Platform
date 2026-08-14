package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	messagesReceived = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "feedhandler_messages_received_total",
			Help: "Total messages received from the exchange, by venue, symbol, and message type.",
		},
		[]string{"venue", "symbol", "type"},
	)

	messagesPublished = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "feedhandler_messages_published_total",
			Help: "Total messages successfully published to Kafka, by venue, symbol, and topic.",
		},
		[]string{"venue", "symbol", "topic"},
	)

	publishErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "feedhandler_publish_errors_total",
			Help: "Kafka publish failures (excluding shutdown), by venue, symbol, and topic.",
		},
		[]string{"venue", "symbol", "topic"},
	)

	parseErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "feedhandler_parse_errors_total",
			Help: "Message parsing failures, by venue and message type.",
		},
		[]string{"venue", "type"},
	)

	ingestLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "feedhandler_ingest_latency_seconds",
			Help:    "Time between exchange timestamp and ingest timestamp.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 12),
		},
		[]string{"venue", "symbol", "type"},
	)

	reconnects = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "feedhandler_reconnects_total",
			Help: "Number of times the exchange WebSocket had to reconnect.",
		},
		[]string{"venue"},
	)
)