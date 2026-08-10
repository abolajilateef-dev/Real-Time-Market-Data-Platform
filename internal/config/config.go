package config

import (
	"os"
	"strings"
)

// Config holds settings common to every service in the platform.
type Config struct {
	KafkaBrokers []string
	DatabaseURL  string
	SymbolsPath  string
	MetricsPort  string
}

// Load reads configuration from environment variables, falling back to defaults
// suitable for local development against the docker-compose stack.

func Load(metricsPort string) Config {
	return Config{
		KafkaBrokers: splitCSV(getEnv("KAFKA_BROKERS", "localhost:19092")),
		DatabaseURL:  getEnv("DATABASE_URL", "postgres://mdp:mdp@localhost:5432/mdp?sslmode=disable"),
		SymbolsPath:  getEnv("SYMBOLS_PATH", "configs/symbols.yaml"),
		MetricsPort:  metricsPort,
	}
}

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}