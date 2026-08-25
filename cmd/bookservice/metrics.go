package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	snapshotsApplied = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bookservice_snapshots_applied_total",
			Help: "Total snapshots loaded, by venue and symbol.",
		},
		[]string{"venue", "symbol"},
	)

	updatesApplied = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bookservice_updates_applied_total",
			Help: "Total book updates successfully applied, by venue and symbol.",
		},
		[]string{"venue", "symbol"},
	)

	updatesStale = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bookservice_updates_stale_total",
			Help: "Total book updates dropped as stale/duplicate, by venue and symbol.",
		},
		[]string{"venue", "symbol"},
	)

	gapsDetected = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bookservice_gaps_detected_total",
			Help: "Total sequence gaps detected, by venue and symbol.",
		},
		[]string{"venue", "symbol"},
	)

	droppedMessages = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bookservice_dropped_messages_total",
			Help: "Messages dropped due to full worker channels, by venue and symbol.",
		},
		[]string{"venue", "symbol"},
	)

	topsEmitted = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bookservice_tops_emitted_total",
			Help: "Total BookTop messages published, by venue and symbol.",
		},
		[]string{"venue", "symbol"},
	)

	publishErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bookservice_publish_errors_total",
			Help: "Kafka publish failures (excluding shutdown), by venue and symbol.",
		},
		[]string{"venue", "symbol"},
	)

	topLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "bookservice_top_latency_seconds",
			Help:    "Time from exchange timestamp to BookTop publish, by venue and symbol.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 12),
		},
		[]string{"venue", "symbol"},
	)
)