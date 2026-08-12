package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/abolajilateef-dev/mdp/internal/config"
	"github.com/abolajilateef-dev/mdp/internal/kafka"
	"github.com/abolajilateef-dev/mdp/internal/obs"
	"github.com/abolajilateef-dev/mdp/pkg/schema"
)

const (
	serviceName = "feedhandler-coinbase"
	metricsPort = "9101"
)

func main() {
	log := obs.NewLogger(serviceName)
	cfg := config.Load(metricsPort)

	log.Info("starting",
		"kafka_brokers", cfg.KafkaBrokers,
		"metrics_port", cfg.MetricsPort,
		"symbols_path", cfg.SymbolsPath,
	)

	symbols, err := schema.LoadSymbols(cfg.SymbolsPath)
	if err != nil {
		log.Error("load symbols", "err", err)
		os.Exit(1)
	}
	log.Info("loaded symbols",
		"count", len(symbols.CanonicalsForVenue("coinbase")),
		"symbols", symbols.CanonicalsForVenue("coinbase"),
	)

	producer, err := kafka.NewProducer(cfg.KafkaBrokers)
	if err != nil {
		log.Error("kafka producer", "err", err)
		os.Exit(1)
	}
	defer producer.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	go func() {
		if err := obs.StartMetricsServer(ctx, log, cfg.MetricsPort); err != nil {
			log.Error("metrics server", "err", err)
		}
	}()

	stream(ctx, log, producer, symbols)

	log.Info("shutdown complete")
}