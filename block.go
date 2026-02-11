package ethwal

import (
	"github.com/0xsequence/ethkit/go-ethereum/common"
)

type Block[T any] struct {
	Hash   common.Hash `json:"blockHash"`
	Parent common.Hash `json:"parentHash"`
	Number uint64      `json:"blockNum"`
	TS     uint64      `json:"blockTS"` // unix ts
	Data   T           `json:"blockData"`
}

func (b Block[T]) IsZero() bool {
	return b.Number == 0 && b.TS == 0 && b.Hash == (common.Hash{}) && b.Parent == (common.Hash{})
}

type Blocks[T any] []Block[T]
