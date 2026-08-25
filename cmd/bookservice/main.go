package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/signal"
	"syscall"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/abolajilateef-dev/mdp/internal/config"
	"github.com/abolajilateef-dev/mdp/internal/kafka"
	"github.com/abolajilateef-dev/mdp/internal/obs"
	"github.com/abolajilateef-dev/mdp/pkg/schema"
)

const (
	serviceName = "bookservice"
	metricsPort = "9103"

	consumerGroup = "bookservice"

	topicBookUpdates   = "book_updates"
	topicBookSnapshots = "book_snapshots"
	topicBookTops      = "book_tops"
)

func main() {
	log := obs.NewLogger(serviceName)
	cfg := config.Load(metricsPort)

	log.Info("starting",
		"kafka_brokers", cfg.KafkaBrokers,
		"metrics_port", cfg.MetricsPort,
	)

	producer, err := kafka.NewProducer(cfg.KafkaBrokers)
	if err != nil {
		log.Error("kafka producer", "err", err)
		os.Exit(1)
	}
	defer producer.Close()

	consumer, err := kafka.NewConsumer(cfg.KafkaBrokers, consumerGroup, topicBookUpdates, topicBookSnapshots)
	if err != nil {
		log.Error("kafka consumer", "err", err)
		os.Exit(1)
	}
	defer consumer.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	go func() {
		if err := obs.StartMetricsServer(ctx, log, cfg.MetricsPort); err != nil {
			log.Error("metrics server", "err", err)
		}
	}()

	manager := newManager(ctx, log, producer, topicBookTops)
	defer manager.stop()

	if err := runConsumer(ctx, log, consumer, manager); err != nil {
		log.Error("consumer failed", "err", err)
		os.Exit(1)
	}

	log.Info("shutdown complete")
}

// runConsumer polls Kafka in a loop, dispatches messages to the manager, and
// commits offsets after each successful batch.
func runConsumer(ctx context.Context, log *slog.Logger, consumer *kafka.Consumer, manager *manager) error {
	for {
		records, err := consumer.Poll(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			log.Error("poll", "err", err)
			continue
		}

		if len(records) == 0 {
			continue
		}

		for _, r := range records {
			dispatch(log, manager, r)
		}

		if err := consumer.Commit(ctx, records...); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			log.Error("commit", "err", err)
		}
	}
}

// dispatch parses one Kafka record based on its topic and hands it to the
// manager to route to the right per-symbol goroutine.
func dispatch(log *slog.Logger, manager *manager, r *kgo.Record) {
	switch r.Topic {
	case topicBookUpdates:
		var upd schema.BookUpdate
		if err := json.Unmarshal(r.Value, &upd); err != nil {
			log.Warn("unmarshal book_update", "err", err)
			return
		}
		manager.handleUpdate(upd)

	case topicBookSnapshots:
		var snap schema.BookSnapshot
		if err := json.Unmarshal(r.Value, &snap); err != nil {
			log.Warn("unmarshal book_snapshot", "err", err)
			return
		}
		manager.handleSnapshot(snap)

	default:
		log.Warn("unknown topic", "topic", r.Topic)
	}
}