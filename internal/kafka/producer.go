package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer publishes JSON-encoded messages to Kafka topics.
type Producer struct {
	client *kgo.Client
}


// Brokers is a slice of "host:port" strings — usually one entry for local dev.
func NewProducer(brokers []string) (*Producer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ProducerLinger(5*time.Millisecond),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}
	return &Producer{client: client}, nil
}


// key controls partitioning: records with the same key land in the same partition,
// preserving ordering per key. For us, key is the canonical symbol.
func (p *Producer) PublishJSON(ctx context.Context, topic, key string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	record := &kgo.Record{
		Topic: topic,
		Key:   []byte(key),
		Value: data,
	}
	return p.client.ProduceSync(ctx, record).FirstErr()
}

// Close flushes any pending records and shuts down the client.
func (p *Producer) Close() {
	p.client.Close()
}