package main

import (
	"github.com/google/btree"

	"github.com/abolajilateef-dev/mdp/pkg/schema"
)

// level is a single price level stored inside the B-tree.
type level struct {
	Price float64
	Size  float64
}

// OrderBook holds one venue+symbol's L2 order book.
// Not goroutine-safe — the manager ensures single-threaded access.
type OrderBook struct {
	Venue    string
	Symbol   string
	Sequence uint64 // last applied sequence

	bids *btree.BTreeG[level]
	asks *btree.BTreeG[level]
}

// NewOrderBook creates an empty book for the given venue and symbol.
func NewOrderBook(venue, symbol string) *OrderBook {
	return &OrderBook{
		Venue:  venue,
		Symbol: symbol,
		// bids sorted descending by price — best (highest) bid is smallest in the tree.
		bids: btree.NewG(32, func(a, b level) bool { return a.Price > b.Price }),
		// asks sorted ascending by price — best (lowest) ask is smallest in the tree.
		asks: btree.NewG(32, func(a, b level) bool { return a.Price < b.Price }),
	}
}

// LoadSnapshot replaces the entire book with data from a snapshot.
// Called on startup and on any book reset (gap recovery).
func (b *OrderBook) LoadSnapshot(snap schema.BookSnapshot) {
	b.bids.Clear(false)
	b.asks.Clear(false)

	for _, lvl := range snap.Bids {
		if lvl.Size > 0 {
			b.bids.ReplaceOrInsert(level{Price: lvl.Price, Size: lvl.Size})
		}
	}
	for _, lvl := range snap.Asks {
		if lvl.Size > 0 {
			b.asks.ReplaceOrInsert(level{Price: lvl.Price, Size: lvl.Size})
		}
	}

	b.Sequence = snap.Sequence
}

// ApplyChanges applies a batch of level changes to the book.
// Size == 0 means delete the level; otherwise insert or overwrite.
func (b *OrderBook) ApplyChanges(changes []schema.LevelChange) {
	for _, c := range changes {
		tree := b.treeFor(c.Side)
		if tree == nil {
			continue
		}
		key := level{Price: c.Price}
		if c.Size == 0 {
			tree.Delete(key)
		} else {
			tree.ReplaceOrInsert(level{Price: c.Price, Size: c.Size})
		}
	}
}

// treeFor returns the tree for the given side, or nil for unknown sides.
func (b *OrderBook) treeFor(side schema.Side) *btree.BTreeG[level] {
	switch side {
	case schema.SideBid:
		return b.bids
	case schema.SideAsk:
		return b.asks
	default:
		return nil
	}
}

// Top returns the best bid and best ask. Any returned (price, size) with
// price == 0 means that side is currently empty.
func (b *OrderBook) Top() (bidPrice, bidSize, askPrice, askSize float64) {
	if best, ok := b.bids.Min(); ok {
		bidPrice = best.Price
		bidSize = best.Size
	}
	if best, ok := b.asks.Min(); ok {
		askPrice = best.Price
		askSize = best.Size
	}
	return
}

// Levels returns up to n levels from the given side, starting from the best.
// Useful for depth queries, VWAP calculations, etc. Not used in Pass 1 but
// costs nothing to expose.
func (b *OrderBook) Levels(side schema.Side, n int) []schema.Level {
	tree := b.treeFor(side)
	if tree == nil || n <= 0 {
		return nil
	}
	out := make([]schema.Level, 0, n)
	tree.Ascend(func(l level) bool {
		out = append(out, schema.Level{Price: l.Price, Size: l.Size})
		return len(out) < n
	})
	return out
}