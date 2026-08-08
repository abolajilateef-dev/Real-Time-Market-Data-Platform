package schema

import "time"

// Side identifies which side of the order book a message pertains to.
type Side uint8

const (
	SideUnknown Side = 0
	SideBid     Side = 1
	SideAsk     Side = 2
)

func (s Side) String() string {
	switch s {
	case SideBid:
		return "bid"
	case SideAsk:
		return "ask"
	default:
		return "unknown"
	}
}

// Level is one price level on one side of the book.
type Level struct {
	Price float64 `json:"price"`
	Size  float64 `json:"size"`
}

// LevelChange is one price-level change inside a BookUpdate.
// Size is the absolute new size at Price on Side. Size == 0 means delete the level.
type LevelChange struct {
	Side  Side    `json:"side"`
	Price float64 `json:"price"`
	Size  float64 `json:"size"`
}

// Trade is a completed transaction reported by an exchange.
type Trade struct {
	Venue      string    `json:"venue"`
	Symbol     string    `json:"symbol"`
	TradeID    string    `json:"trade_id"`
	Price      float64   `json:"price"`
	Size       float64   `json:"size"`
	Side       Side      `json:"side"`
	ExchangeTs time.Time `json:"exchange_ts"`
	IngestTs   time.Time `json:"ingest_ts"`
}

// BookUpdate is one atomic book event from an exchange: one or more level
// changes, all sharing the same sequence number.
type BookUpdate struct {
	Venue      string        `json:"venue"`
	Symbol     string        `json:"symbol"`
	Changes    []LevelChange `json:"changes"`
	Sequence   uint64        `json:"sequence"`
	ExchangeTs time.Time     `json:"exchange_ts"`
	IngestTs   time.Time     `json:"ingest_ts"`
}

// BookSnapshot is the full book at a point in time, as reported by an exchange.
type BookSnapshot struct {
	Venue      string    `json:"venue"`
	Symbol     string    `json:"symbol"`
	Bids       []Level   `json:"bids"`
	Asks       []Level   `json:"asks"`
	Sequence   uint64    `json:"sequence"`
	ExchangeTs time.Time `json:"exchange_ts"`
	IngestTs   time.Time `json:"ingest_ts"`
}

// BookTop is emitted by the Book Service after applying updates.
// It's the maintained best bid and best ask for downstream consumers.
type BookTop struct {
	Venue      string    `json:"venue"`
	Symbol     string    `json:"symbol"`
	BidPrice   float64   `json:"bid_price"`
	BidSize    float64   `json:"bid_size"`
	AskPrice   float64   `json:"ask_price"`
	AskSize    float64   `json:"ask_size"`
	Sequence   uint64    `json:"sequence"`
	ExchangeTs time.Time `json:"exchange_ts"`
	EmitTs     time.Time `json:"emit_ts"`
}