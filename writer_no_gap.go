package ethwal

import (
	"context"

	"github.com/0xsequence/ethwal/storage"
)

type noGapWriter[T any] struct {
	w Writer[T]

	lastBlockNum uint64
}

func NewWriterNoGap[T any](w Writer[T]) Writer[T] {
	return &noGapWriter[T]{w: w, lastBlockNum: w.BlockNum()}
}

func (n *noGapWriter[T]) FileSystem() storage.FS {
	return n.w.FileSystem()
}

func (n *noGapWriter[T]) Write(ctx context.Context, b Block[T]) error {
	defer func() { n.lastBlockNum = b.Number }()

	// skip if block number is less than or equal to last block number
	if n.lastBlockNum != NoBlockNum && b.Number <= n.lastBlockNum {
		return nil
	}

	// write blocks as there is no gap
	if b.Number == n.lastBlockNum+1 {
		return n.w.Write(ctx, b)
	}

	// write missing blocks
	for i := n.lastBlockNum + 1; i < b.Number; i++ {
		err := n.w.Write(ctx, Block[T]{Number: i})
		if err != nil {
			return err
		}
	}

	return n.w.Write(ctx, b)
}

func (n *noGapWriter[T]) RollFile(ctx context.Context) error {
	return n.w.RollFile(ctx)
}

func (n *noGapWriter[T]) BlockNum() uint64 {
	return n.w.BlockNum()
}

func (n *noGapWriter[T]) Close(ctx context.Context) error {
	return n.w.Close(ctx)
}

func (n *noGapWriter[T]) Options() Options {
	return n.w.Options()
}

func (n *noGapWriter[T]) SetOptions(opts Options) {
	n.w.SetOptions(opts)
}
