package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/abolajilateef-dev/mdp/internal/kafka"
	"github.com/abolajilateef-dev/mdp/pkg/schema"
)

const (
	testUpdatesTopic   = "book_updates_test"
	testSnapshotsTopic = "book_snapshots_test"
	testTopsTopic      = "book_tops_test"
)

func TestEndToEndPipeline(t *testing.T) {
	if os.Getenv("MDP_INTEGRATION") != "1" {
		t.Skip("set MDP_INTEGRATION=1 to run (requires `make up` + `make kafka-topics-test`)")
	}

	brokers := []string{"localhost:19092"}
	if b := os.Getenv("KAFKA_BROKERS"); b != "" {
		brokers = []string{b}
	}

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))
	symbol := fmt.Sprintf("IT-BTC-USD-%d", time.Now().UnixNano())
	runID := time.Now().UnixNano()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Producer: stands in for the feed handler.
	producer, err := kafka.NewProducer(brokers)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer producer.Close()

	// Consumer feeding the real manager (book service logic under test).
	inGroup := fmt.Sprintf("it-bookservice-%d", runID)
	inConsumer, err := kafka.NewConsumer(brokers, inGroup, testUpdatesTopic, testSnapshotsTopic)
	if err != nil {
		t.Fatalf("in consumer: %v", err)
	}
	defer inConsumer.Close()

	// Consumer verifying downstream output.
	topsGroup := fmt.Sprintf("it-topsverify-%d", runID)
	topsConsumer, err := kafka.NewConsumer(brokers, topsGroup, testTopsTopic)
	if err != nil {
		t.Fatalf("tops consumer: %v", err)
	}
	defer topsConsumer.Close()

	mgr := newManager(ctx, log, producer, testTopsTopic)
	defer mgr.stop()

	// Drive the real manager off real Kafka messages.
	go func() {
		for {
			records, err := inConsumer.Poll(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				continue
			}
			for _, r := range records {
				dispatchTest(log, mgr, r)
			}
			_ = inConsumer.Commit(ctx, records...)
		}
	}()

	// Collect BookTop output for assertions.
	topsCh := make(chan schema.BookTop, 100)
	go func() {
		for {
			records, err := topsConsumer.Poll(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				continue
			}
			for _, r := range records {
				var top schema.BookTop
				if err := json.Unmarshal(r.Value, &top); err != nil {
					continue
				}
				select {
				case topsCh <- top:
				default:
				}
			}
			_ = topsConsumer.Commit(ctx, records...)
		}
	}()

	now := time.Now().UTC()

	// ---- Phase 1: ingestion → maintained book → published top ----
	t.Log("phase 1: publishing initial snapshot")
	publish(t, ctx, producer, testSnapshotsTopic, symbol, schema.BookSnapshot{
		Venue:      "coinbase",
		Symbol:     symbol,
		Bids:       []schema.Level{{Price: 100, Size: 1}},
		Asks:       []schema.Level{{Price: 101, Size: 1}},
		Sequence:   1,
		ExchangeTs: now,
		IngestTs:   now,
	})
	top := waitForTop(t, topsCh, symbol, 1, 10*time.Second)
	assertTop(t, top, 100, 1, 101, 1)
	t.Log("phase 1: snapshot correctly reflected in first BookTop")

	t.Log("phase 1: publishing delta seq=2 (ask improves)")
	publish(t, ctx, producer, testUpdatesTopic, symbol, schema.BookUpdate{
		Venue:  "coinbase",
		Symbol: symbol,
		Changes: []schema.LevelChange{
			{Side: schema.SideAsk, Price: 101, Size: 0},
			{Side: schema.SideAsk, Price: 102, Size: 1.5},
		},
		Sequence:   2,
		ExchangeTs: now,
		IngestTs:   now,
	})
	top = waitForTop(t, topsCh, symbol, 2, 10*time.Second)
	assertTop(t, top, 100, 1, 102, 1.5)
	t.Log("phase 1: delta correctly applied and republished")

	t.Log("phase 1: publishing delta seq=3 (bid improves)")
	publish(t, ctx, producer, testUpdatesTopic, symbol, schema.BookUpdate{
		Venue:      "coinbase",
		Symbol:     symbol,
		Changes:    []schema.LevelChange{{Side: schema.SideBid, Price: 105, Size: 3}},
		Sequence:   3,
		ExchangeTs: now,
		IngestTs:   now,
	})
	top = waitForTop(t, topsCh, symbol, 3, 10*time.Second)
	assertTop(t, top, 105, 3, 102, 1.5)
	t.Log("phase 1 complete: happy path verified end to end")

	// ---- Phase 2: sequence gap injection and recovery ----
	t.Log("phase 2: injecting sequence gap (jumping from 3 to 10)")
	publish(t, ctx, producer, testUpdatesTopic, symbol, schema.BookUpdate{
		Venue:      "coinbase",
		Symbol:     symbol,
		Changes:    []schema.LevelChange{{Side: schema.SideBid, Price: 106, Size: 1}},
		Sequence:   10,
		ExchangeTs: now,
		IngestTs:   now,
	})
	assertNoTopWithin(t, topsCh, symbol, 2*time.Second)
	t.Log("phase 2: confirmed no corrupted BookTop was published during the gap window")

	t.Log("phase 2: publishing recovery snapshot seq=20")
	publish(t, ctx, producer, testSnapshotsTopic, symbol, schema.BookSnapshot{
		Venue:      "coinbase",
		Symbol:     symbol,
		Bids:       []schema.Level{{Price: 200, Size: 2}},
		Asks:       []schema.Level{{Price: 201, Size: 2}},
		Sequence:   20,
		ExchangeTs: now,
		IngestTs:   now,
	})
	top = waitForTop(t, topsCh, symbol, 20, 10*time.Second)
	assertTop(t, top, 200, 2, 201, 2)
	t.Log("phase 2: recovery confirmed — book reset cleanly to the new snapshot")

	t.Log("phase 2: publishing post-recovery delta seq=21")
	publish(t, ctx, producer, testUpdatesTopic, symbol, schema.BookUpdate{
		Venue:      "coinbase",
		Symbol:     symbol,
		Changes: []schema.LevelChange{
    							{Side: schema.SideAsk, Price: 201, Size: 0}, // remove the resting ask from the snapshot
    							{Side: schema.SideAsk, Price: 202, Size: 4}, // new best ask
								},
		Sequence:   21,
		ExchangeTs: now,
		IngestTs:   now,
	})
	top = waitForTop(t, topsCh, symbol, 21, 10*time.Second)
	assertTop(t, top, 200, 2, 202, 4)
	t.Log("phase 2 complete: pipeline resumed normal operation after recovery")

	t.Log("END TO END: ingestion -> kafka -> book service -> gap detection -> snapshot recovery -> kafka (book_tops_test) all verified")
}

func dispatchTest(log *slog.Logger, m *manager, r *kgo.Record) {
	switch r.Topic {
	case testUpdatesTopic:
		var upd schema.BookUpdate
		if err := json.Unmarshal(r.Value, &upd); err != nil {
			log.Warn("test: unmarshal update", "err", err)
			return
		}
		m.handleUpdate(upd)
	case testSnapshotsTopic:
		var snap schema.BookSnapshot
		if err := json.Unmarshal(r.Value, &snap); err != nil {
			log.Warn("test: unmarshal snapshot", "err", err)
			return
		}
		m.handleSnapshot(snap)
	}
}

func publish(t *testing.T, ctx context.Context, p *kafka.Producer, topic, key string, v any) {
	t.Helper()
	if err := p.PublishJSON(ctx, topic, key, v); err != nil {
		t.Fatalf("publish to %s: %v", topic, err)
	}
}

func waitForTop(t *testing.T, tops <-chan schema.BookTop, symbol string, seq uint64, timeout time.Duration) schema.BookTop {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case top := <-tops:
			if top.Symbol == symbol && top.Sequence == seq {
				return top
			}
		case <-deadline:
			t.Fatalf("timed out waiting for BookTop symbol=%s seq=%d", symbol, seq)
		}
	}
}

func assertNoTopWithin(t *testing.T, tops <-chan schema.BookTop, symbol string, d time.Duration) {
	t.Helper()
	timer := time.NewTimer(d)
	defer timer.Stop()
	for {
		select {
		case top := <-tops:
			if top.Symbol == symbol {
				t.Fatalf("unexpected BookTop published during gap window: seq=%d bid=%v ask=%v", top.Sequence, top.BidPrice, top.AskPrice)
			}
		case <-timer.C:
			return
		}
	}
}

func assertTop(t *testing.T, top schema.BookTop, bidP, bidS, askP, askS float64) {
	t.Helper()
	if top.BidPrice != bidP || top.BidSize != bidS {
		t.Errorf("bid: want (%v, %v), got (%v, %v)", bidP, bidS, top.BidPrice, top.BidSize)
	}
	if top.AskPrice != askP || top.AskSize != askS {
		t.Errorf("ask: want (%v, %v), got (%v, %v)", askP, askS, top.AskPrice, top.AskSize)
	}
}