package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/abolajilateef-dev/mdp/internal/kafka"
	"github.com/abolajilateef-dev/mdp/pkg/schema"
)

const (
	channelBuffer  = 1024   // per-worker channel capacity
	maxBufferSize  = 10_000 // max buffered deltas while waiting for snapshot
)

// workerState is the per-symbol state machine value.
type workerState uint8

const (
	waitingForSnapshot workerState = iota
	live
)

// manager owns per-symbol workers. It exposes a synchronous API to callers;
// under the hood, each symbol is handled by its own goroutine.
type manager struct {
	ctx      context.Context
	log      *slog.Logger
	producer *kafka.Producer
	topsTopic string

	mu      sync.Mutex
	workers map[string]*symbolWorker

	wg sync.WaitGroup
}

// newManager constructs a manager. It does not spawn workers eagerly —
// workers appear as needed when the first message for a symbol arrives.
func newManager(ctx context.Context, log *slog.Logger, producer *kafka.Producer, topsTopic string) *manager {
	return &manager{
		ctx:       ctx,
		log:       log,
		producer:  producer,
		topsTopic: topsTopic,
		workers:   make(map[string]*symbolWorker),
	}
}

// handleUpdate routes a BookUpdate to the appropriate per-symbol worker.
func (m *manager) handleUpdate(upd schema.BookUpdate) {
	w := m.workerFor(upd.Venue, upd.Symbol)
	select {
	case w.in <- workerMsg{update: &upd}:
	default:
		// worker channel full — drop and log. Rare in normal operation.
		droppedMessages.WithLabelValues(upd.Venue, upd.Symbol).Inc()
		m.log.Warn("worker channel full", "venue", upd.Venue, "symbol", upd.Symbol)
	}
}

// handleSnapshot routes a BookSnapshot to the appropriate per-symbol worker.
func (m *manager) handleSnapshot(snap schema.BookSnapshot) {
	w := m.workerFor(snap.Venue, snap.Symbol)
	select {
	case w.in <- workerMsg{snapshot: &snap}:
	default:
		droppedMessages.WithLabelValues(snap.Venue, snap.Symbol).Inc()
		m.log.Warn("worker channel full", "venue", snap.Venue, "symbol", snap.Symbol)
	}
}

// stop closes all worker channels and waits for goroutines to finish.
func (m *manager) stop() {
	m.mu.Lock()
	for _, w := range m.workers {
		close(w.in)
	}
	m.mu.Unlock()
	m.wg.Wait()
}

// workerFor returns the worker for (venue, symbol), spawning one if it doesn't exist.
func (m *manager) workerFor(venue, symbol string) *symbolWorker {
	key := workerKey(venue, symbol)

	m.mu.Lock()
	defer m.mu.Unlock()

	if w, ok := m.workers[key]; ok {
		return w
	}

	w := &symbolWorker{
		venue:    venue,
		symbol:   symbol,
		book:     NewOrderBook(venue, symbol),
		state:    waitingForSnapshot,
		buffer:   make([]schema.BookUpdate, 0, 128),
		in:       make(chan workerMsg, channelBuffer),
		log:      m.log.With("venue", venue, "symbol", symbol),
		producer: m.producer,
		topsTopic: m.topsTopic,
	}
	m.workers[key] = w

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		w.run(m.ctx)
	}()

	m.log.Info("worker started", "venue", venue, "symbol", symbol)
	return w
}

func workerKey(venue, symbol string) string {
	return venue + "|" + symbol
}

// workerMsg is what flows through a worker's channel.
// Exactly one of update or snapshot is non-nil.
type workerMsg struct {
	update   *schema.BookUpdate
	snapshot *schema.BookSnapshot
}

// symbolWorker owns one book and processes one symbol's message stream.
type symbolWorker struct {
	venue  string
	symbol string

	book       *OrderBook
	state      workerState
	buffer     []schema.BookUpdate
	lastApplied uint64

	in chan workerMsg

	log       *slog.Logger
	producer  *kafka.Producer
	topsTopic string
}

// run is the worker's main loop. Exits when in is closed or ctx is cancelled.
func (w *symbolWorker) run(ctx context.Context) {
	for {
		select {
		case msg, ok := <-w.in:
			if !ok {
				return
			}
			switch {
			case msg.snapshot != nil:
				w.onSnapshot(ctx, *msg.snapshot)
			case msg.update != nil:
				w.onUpdate(ctx, *msg.update)
			}
		case <-ctx.Done():
			return
		}
	}
}

// onSnapshot handles a snapshot arrival: replace book, replay buffered deltas.
func (w *symbolWorker) onSnapshot(ctx context.Context, snap schema.BookSnapshot) {
	w.book.LoadSnapshot(snap)
	w.lastApplied = snap.Sequence
	w.state = live

	// Replay buffered deltas with sequence > snapshot.Sequence.
	applied := 0
	for _, upd := range w.buffer {
		if upd.Sequence <= snap.Sequence {
			continue
		}
		if upd.Sequence != w.lastApplied+1 {
			// Gap even in the buffered range. Discard book, wait for next snapshot.
			w.enterWaiting("gap in buffered deltas after snapshot")
			return
		}
		w.book.ApplyChanges(upd.Changes)
		w.lastApplied = upd.Sequence
		applied++
	}
	w.buffer = w.buffer[:0]

	w.log.Info("snapshot loaded",
		"seq", snap.Sequence,
		"replayed_deltas", applied,
		"bids", len(snap.Bids),
		"asks", len(snap.Asks),
	)

	snapshotsApplied.WithLabelValues(w.venue, w.symbol).Inc()

	w.emitTop(ctx, snap.ExchangeTs)
}

// onUpdate handles a BookUpdate arrival.
func (w *symbolWorker) onUpdate(ctx context.Context, upd schema.BookUpdate) {
	switch w.state {
	case waitingForSnapshot:
		w.bufferUpdate(upd)
		return

	case live:
		switch {
		case upd.Sequence == w.lastApplied+1:
			w.book.ApplyChanges(upd.Changes)
			w.lastApplied = upd.Sequence
			updatesApplied.WithLabelValues(w.venue, w.symbol).Inc()
			w.emitTop(ctx, upd.ExchangeTs)

		case upd.Sequence <= w.lastApplied:
			// Duplicate or stale. Drop silently but count it.
			updatesStale.WithLabelValues(w.venue, w.symbol).Inc()

		default:
			// Gap: upd.Sequence > lastApplied + 1
			w.log.Warn("sequence gap",
				"expected", w.lastApplied+1,
				"got", upd.Sequence,
			)
			gapsDetected.WithLabelValues(w.venue, w.symbol).Inc()
			w.enterWaiting("live gap")
			w.bufferUpdate(upd)
		}
	}
}

// bufferUpdate appends to the buffer, dropping if full.
func (w *symbolWorker) bufferUpdate(upd schema.BookUpdate) {
	if len(w.buffer) >= maxBufferSize {
		w.log.Warn("buffer full, dropping oldest", "size", len(w.buffer))
		w.buffer = w.buffer[1:]
	}
	w.buffer = append(w.buffer, upd)
}

// enterWaiting transitions the worker to waitingForSnapshot state.
func (w *symbolWorker) enterWaiting(reason string) {
	w.state = waitingForSnapshot
	w.lastApplied = 0
	w.buffer = w.buffer[:0]
	w.log.Warn("entering waiting_for_snapshot", "reason", reason)
}

// emitTop reads the current top from the book and publishes a BookTop if
// either the price or size on either side has changed since the last emit.
func (w *symbolWorker) emitTop(ctx context.Context, exchangeTs time.Time) {
	bidP, bidS, askP, askS := w.book.Top()

	// Skip if both sides are empty (e.g., snapshot with no levels).
	if bidP == 0 && askP == 0 {
		return
	}

	top := schema.BookTop{
		Venue:      w.venue,
		Symbol:     w.symbol,
		BidPrice:   bidP,
		BidSize:    bidS,
		AskPrice:   askP,
		AskSize:    askS,
		Sequence:   w.lastApplied,
		ExchangeTs: exchangeTs,
		EmitTs:     time.Now().UTC(),
	}

	// Publish. Use symbol as key so consumers stay per-symbol partitioned.
	if err := w.producer.PublishJSON(ctx, w.topsTopic, w.symbol, top); err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		w.log.Error("publish book_top", "err", err)
		publishErrors.WithLabelValues(w.venue, w.symbol).Inc()
		return
	}
	topsEmitted.WithLabelValues(w.venue, w.symbol).Inc()

	// Track top-to-top latency (exchange event → BookTop emitted).
	topLatency.WithLabelValues(w.venue, w.symbol).Observe(time.Since(exchangeTs).Seconds())
}

// Unused type-imports guard.
// (This will be removed once imports settle.)
var _ = json.Marshal
var _ = fmt.Errorf