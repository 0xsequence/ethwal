package ethlogwal

type Block[T any] struct {
	BlockHash   [32]byte `json:"blockHash"`
	BlockNumber uint64   `json:"blockNum"`
	TS          uint64   `json:"blockTS"` // unix ts
	Data        T        `json:"data"`
}

type Blocks[T any] []Block[T]
