package ethwal

import (
	"github.com/0xsequence/ethkit/go-ethereum/common"
)

type Block[T any] struct {
	Hash   common.Hash `json:"blockHash"`
	Number uint64      `json:"onBlockProcessed"`
	TS     uint64      `json:"blockTS"` // unix ts
	Data   T           `json:"blockData"`
}

type Blocks[T any] []Block[T]
