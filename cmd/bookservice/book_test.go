package main

import (
	"testing"

	"github.com/abolajilateef-dev/mdp/pkg/schema"
)

func TestOrderBook_LoadSnapshot(t *testing.T) {
	b := NewOrderBook("coinbase", "BTC-USD")
	snap := schema.BookSnapshot{
		Bids:     []schema.Level{{Price: 60000, Size: 1.5}, {Price: 59999, Size: 2.0}},
		Asks:     []schema.Level{{Price: 60001, Size: 1.0}, {Price: 60002, Size: 3.0}},
		Sequence: 42,
	}
	b.LoadSnapshot(snap)

	bidP, bidS, askP, askS := b.Top()
	if bidP != 60000 || bidS != 1.5 {
		t.Errorf("best bid: want (60000, 1.5), got (%v, %v)", bidP, bidS)
	}
	if askP != 60001 || askS != 1.0 {
		t.Errorf("best ask: want (60001, 1.0), got (%v, %v)", askP, askS)
	}
	if b.Sequence != 42 {
		t.Errorf("sequence: want 42, got %d", b.Sequence)
	}
}

func TestOrderBook_ApplyChanges_UpdateAndDelete(t *testing.T) {
	b := NewOrderBook("coinbase", "BTC-USD")
	b.LoadSnapshot(schema.BookSnapshot{
		Bids:     []schema.Level{{Price: 60000, Size: 1.5}},
		Asks:     []schema.Level{{Price: 60001, Size: 1.0}},
		Sequence: 1,
	})

	b.ApplyChanges([]schema.LevelChange{
		{Side: schema.SideBid, Price: 60000, Size: 2.5}, // update
		{Side: schema.SideAsk, Price: 60001, Size: 0},   // delete
		{Side: schema.SideAsk, Price: 60005, Size: 0.7}, // new
	})

	bidP, bidS, askP, askS := b.Top()
	if bidP != 60000 || bidS != 2.5 {
		t.Errorf("best bid after update: want (60000, 2.5), got (%v, %v)", bidP, bidS)
	}
	if askP != 60005 || askS != 0.7 {
		t.Errorf("best ask after delete+new: want (60005, 0.7), got (%v, %v)", askP, askS)
	}
}

func TestOrderBook_Levels(t *testing.T) {
	b := NewOrderBook("coinbase", "BTC-USD")
	b.LoadSnapshot(schema.BookSnapshot{
		Bids: []schema.Level{
			{Price: 60000, Size: 1.5},
			{Price: 59999, Size: 2.0},
			{Price: 59998, Size: 0.5},
			{Price: 59997, Size: 3.0},
		},
	})

	levels := b.Levels(schema.SideBid, 3)
	if len(levels) != 3 {
		t.Fatalf("want 3 levels, got %d", len(levels))
	}
	if levels[0].Price != 60000 || levels[1].Price != 59999 || levels[2].Price != 59998 {
		t.Errorf("bid levels wrong order: %v", levels)
	}
}