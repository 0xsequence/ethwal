package ethwal

import (
	"context"

	"github.com/0xsequence/ethwal/storage"
)

type writerWithFilter[T any] struct {
	writer  Writer[T]
	indexes map[IndexName]Index[T]
}

var _ Writer[any] = (*writerWithFilter[any])(nil)

func NewWriterWithIndexBuilder[T any](writer Writer[T], indexes Indexes[T], fs storage.FS) (Writer[T], error) {
	indexMap := make(map[IndexName]Index[T])
	for name, indexFunc := range indexes {
		idx := &index[T]{
			name:      name.Normalize(),
			indexFunc: indexFunc,
			fs:        fs,
		}
		indexMap[name] = idx
	}

	return &writerWithFilter[T]{indexes: indexMap, writer: writer}, nil
}

func (c *writerWithFilter[T]) Write(ctx context.Context, block Block[T]) error {
	err := c.writer.Write(ctx, block)
	if err != nil {
		return err
	}
	c.store(ctx, block)
	return nil
}

func (c *writerWithFilter[T]) Close(ctx context.Context) error {
	return c.writer.Close(ctx)
}

func (c *writerWithFilter[T]) BlockNum() uint64 {
	return c.writer.BlockNum()
}

func (c *writerWithFilter[T]) RollFile(ctx context.Context) error {
	return c.writer.RollFile(ctx)
}

func (c *writerWithFilter[T]) store(ctx context.Context, block Block[T]) {
	for _, index := range c.indexes {
		index.Store(ctx, block)
	}
}
