package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/abolajilateef-dev/mdp/internal/kafka"
	"github.com/abolajilateef-dev/mdp/pkg/schema"
)

const (
	topicTrades        = "trades"
	topicBookSnapshots = "book_snapshots"
	topicBookUpdates   = "book_updates"
)

// sequenceTracker holds a per-symbol monotonic counter used to stamp outbound
// BookUpdates. Reset on snapshot (new connection or resync).
type sequenceTracker struct {
	seq map[string]uint64
}

func newSequenceTracker() *sequenceTracker {
	return &sequenceTracker{seq: make(map[string]uint64)}
}

func (s *sequenceTracker) reset(symbol string) {
	s.seq[symbol] = 0
}

func (s *sequenceTracker) next(symbol string) uint64 {
	s.seq[symbol]++
	return s.seq[symbol]
}

// -------- match ("trade") --------

type coinbaseMatch struct {
	Type      string `json:"type"`
	TradeID   int64  `json:"trade_id"`
	ProductID string `json:"product_id"`
	Price     string `json:"price"`
	Size      string `json:"size"`
	Side      string `json:"side"` // maker's side; taker's side is the inverse
	Time      string `json:"time"`
}

func handleMatch(
	ctx context.Context,
	log *slog.Logger,
	producer *kafka.Producer,
	symbols *schema.SymbolMap,
	data []byte,
) {
	var m coinbaseMatch
	if err := json.Unmarshal(data, &m); err != nil {
		log.Warn("parse match", "err", err)
		return
	}

	canonical, ok := symbols.CanonicalSymbol(m.ProductID, "coinbase")
	if !ok {
		log.Warn("unknown coinbase product", "product_id", m.ProductID)
		return
	}

	price, err := strconv.ParseFloat(m.Price, 64)
	if err != nil {
		log.Warn("parse price", "err", err, "value", m.Price)
		return
	}
	size, err := strconv.ParseFloat(m.Size, 64)
	if err != nil {
		log.Warn("parse size", "err", err, "value", m.Size)
		return
	}
	exchangeTs, err := time.Parse(time.RFC3339Nano, m.Time)
	if err != nil {
		log.Warn("parse time", "err", err, "value", m.Time)
		return
	}

	trade := schema.Trade{
		Venue:      "coinbase",
		Symbol:     canonical,
		TradeID:    strconv.FormatInt(m.TradeID, 10),
		Price:      price,
		Size:       size,
		Side:       takerSideFromMaker(m.Side),
		ExchangeTs: exchangeTs,
		IngestTs:   time.Now().UTC(),
	}

	if err := producer.PublishJSON(ctx, topicTrades, canonical, trade); err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		log.Error("publish trade", "err", err, "symbol", canonical)
	}
}

// takerSideFromMaker flips Coinbase's maker-side into aggressor (taker) side.
func takerSideFromMaker(makerSide string) schema.Side {
	switch makerSide {
	case "buy":
		return schema.SideAsk // maker was buying → taker was selling
	case "sell":
		return schema.SideBid // maker was selling → taker was buying
	default:
		return schema.SideUnknown
	}
}

// -------- snapshot --------

type coinbaseSnapshot struct {
	Type      string     `json:"type"`
	ProductID string     `json:"product_id"`
	Bids      [][]string `json:"bids"`
	Asks      [][]string `json:"asks"`
}

func handleSnapshot(
	ctx context.Context,
	log *slog.Logger,
	producer *kafka.Producer,
	symbols *schema.SymbolMap,
	tracker *sequenceTracker,
	data []byte,
) {
	var s coinbaseSnapshot
	if err := json.Unmarshal(data, &s); err != nil {
		log.Warn("parse snapshot", "err", err)
		return
	}

	canonical, ok := symbols.CanonicalSymbol(s.ProductID, "coinbase")
	if !ok {
		log.Warn("unknown coinbase product", "product_id", s.ProductID)
		return
	}

	bids, err := parseLevels(s.Bids)
	if err != nil {
		log.Warn("parse snapshot bids", "err", err, "symbol", canonical)
		return
	}
	asks, err := parseLevels(s.Asks)
	if err != nil {
		log.Warn("parse snapshot asks", "err", err, "symbol", canonical)
		return
	}

	tracker.reset(canonical)
	seq := tracker.next(canonical)
	now := time.Now().UTC()

	snapshot := schema.BookSnapshot{
		Venue:      "coinbase",
		Symbol:     canonical,
		Bids:       bids,
		Asks:       asks,
		Sequence:   seq,
		ExchangeTs: now,
		IngestTs:   now,
	}

	if err := producer.PublishJSON(ctx, topicBookSnapshots, canonical, snapshot); err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		log.Error("publish snapshot", "err", err, "symbol", canonical)
		return
	}
	log.Info("snapshot published",
		"symbol", canonical,
		"bids", len(bids),
		"asks", len(asks),
		"seq", seq,
	)
}

// parseLevels converts Coinbase's [[price, size], ...] into []schema.Level.
func parseLevels(raw [][]string) ([]schema.Level, error) {
	out := make([]schema.Level, 0, len(raw))
	for i, row := range raw {
		if len(row) < 2 {
			return nil, fmt.Errorf("row %d: need [price, size], got %d fields", i, len(row))
		}
		price, err := strconv.ParseFloat(row[0], 64)
		if err != nil {
			return nil, fmt.Errorf("row %d: price %q: %w", i, row[0], err)
		}
		size, err := strconv.ParseFloat(row[1], 64)
		if err != nil {
			return nil, fmt.Errorf("row %d: size %q: %w", i, row[1], err)
		}
		out = append(out, schema.Level{Price: price, Size: size})
	}
	return out, nil
}

// -------- l2update --------

type coinbaseL2Update struct {
	Type      string     `json:"type"`
	ProductID string     `json:"product_id"`
	Time      string     `json:"time"`
	Changes   [][]string `json:"changes"`
}

func handleL2Update(
	ctx context.Context,
	log *slog.Logger,
	producer *kafka.Producer,
	symbols *schema.SymbolMap,
	tracker *sequenceTracker,
	data []byte,
) {
	var u coinbaseL2Update
	if err := json.Unmarshal(data, &u); err != nil {
		log.Warn("parse l2update", "err", err)
		return
	}

	canonical, ok := symbols.CanonicalSymbol(u.ProductID, "coinbase")
	if !ok {
		log.Warn("unknown coinbase product", "product_id", u.ProductID)
		return
	}

	changes, err := parseChanges(u.Changes)
	if err != nil {
		log.Warn("parse l2update changes", "err", err, "symbol", canonical)
		return
	}

	exchangeTs, err := time.Parse(time.RFC3339Nano, u.Time)
	if err != nil {
		log.Warn("parse l2update time", "err", err, "value", u.Time)
		return
	}

	seq := tracker.next(canonical)

	update := schema.BookUpdate{
		Venue:      "coinbase",
		Symbol:     canonical,
		Changes:    changes,
		Sequence:   seq,
		ExchangeTs: exchangeTs,
		IngestTs:   time.Now().UTC(),
	}

	if err := producer.PublishJSON(ctx, topicBookUpdates, canonical, update); err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		log.Error("publish l2update", "err", err, "symbol", canonical, "seq", seq)
	}
}

// parseChanges converts Coinbase's [[side, price, size], ...] into []schema.LevelChange.
func parseChanges(raw [][]string) ([]schema.LevelChange, error) {
	out := make([]schema.LevelChange, 0, len(raw))
	for i, row := range raw {
		if len(row) < 3 {
			return nil, fmt.Errorf("row %d: need [side, price, size], got %d fields", i, len(row))
		}
		var side schema.Side
		switch row[0] {
		case "buy":
			side = schema.SideBid
		case "sell":
			side = schema.SideAsk
		default:
			return nil, fmt.Errorf("row %d: unknown side %q", i, row[0])
		}
		price, err := strconv.ParseFloat(row[1], 64)
		if err != nil {
			return nil, fmt.Errorf("row %d: price %q: %w", i, row[1], err)
		}
		size, err := strconv.ParseFloat(row[2], 64)
		if err != nil {
			return nil, fmt.Errorf("row %d: size %q: %w", i, row[2], err)
		}
		out = append(out, schema.LevelChange{Side: side, Price: price, Size: size})
	}
	return out, nil
}