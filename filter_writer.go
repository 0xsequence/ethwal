package ethwal

import (
	"context"

	"github.com/0xsequence/ethwal/storage"
)

type chainLensWriter[T any] struct {
	writer  Writer[T]
	indexes map[IndexName]Index[T]
}

var _ Writer[any] = (*chainLensWriter[any])(nil)

func NewChainLensWriter[T any](writer Writer[T], indexes Indexes[T], fs storage.FS) (Writer[T], error) {
	indexMap := make(map[IndexName]Index[T])
	for name, indexFunc := range indexes {
		idx := &index[T]{
			name:      name.Normalize(),
			indexFunc: indexFunc,
			fs:        fs,
		}
		indexMap[name] = idx
	}
	cl := &chainLensWriter[T]{indexes: indexMap}
	writer.SetCallback(cl.callback)
	cl.writer = writer
	return cl, nil
}

func (c *chainLensWriter[T]) Write(ctx context.Context, block Block[T]) error {
	// fmt.Println("chainLensWriter Write called", block.Number)
	return c.writer.Write(ctx, block)
}

func (c *chainLensWriter[T]) Close(ctx context.Context) error {
	return c.writer.Close(ctx)
}

func (c *chainLensWriter[T]) BlockNum() uint64 {
	return c.writer.BlockNum()
}

func (c *chainLensWriter[T]) RollFile(ctx context.Context) error {
	return c.writer.RollFile(ctx)
}

func (c *chainLensWriter[T]) SetCallback(cb func(Block[T])) {
}

func (c *chainLensWriter[T]) callback(block Block[T]) {
	for _, index := range c.indexes {
		// TODO: what should the context be?
		index.Store(context.Background(), block)
	}
}
