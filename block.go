package ethlogwal

import "github.com/0xsequence/go-sequence/lib/prototyp"

type Block[T any] struct {
	Hash   prototyp.Hash `json:"blockHash"`
	Number uint64        `json:"blockNum"`
	TS     uint64        `json:"blockTS"` // unix ts
	Data   T             `json:"blockData"`
}

type Blocks[T any] []Block[T]
