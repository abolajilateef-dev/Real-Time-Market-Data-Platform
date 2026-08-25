package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Consumer reads records from Kafka topics as part of a consumer group.
type Consumer struct {
	client *kgo.Client
}

// NewConsumer joins the given consumer group and subscribes to the given topics.
// Offsets are auto-committed periodically by the underlying client.
func NewConsumer(brokers []string, group string, topics ...string) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxBytes(5*1024*1024),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer: %w", err)
	}
	return &Consumer{client: client}, nil
}

// Poll waits for the next batch of records or until ctx is cancelled.
// Returns the raw records so the caller can decide how to deserialize them.
func (c *Consumer) Poll(ctx context.Context) ([]*kgo.Record, error) {
	fetches := c.client.PollFetches(ctx)
	if errs := fetches.Errors(); len(errs) > 0 {
		return nil, fmt.Errorf("poll: %w", errs[0].Err)
	}

	var records []*kgo.Record
	fetches.EachRecord(func(r *kgo.Record) {
		records = append(records, r)
	})
	return records, nil
}

// Commit marks the given records as processed. Call this after successful handling
// so we don't reprocess them on restart.
func (c *Consumer) Commit(ctx context.Context, records ...*kgo.Record) error {
	return c.client.CommitRecords(ctx, records...)
}

// Close leaves the consumer group and shuts down the client.
func (c *Consumer) Close() {
	c.client.Close()
}