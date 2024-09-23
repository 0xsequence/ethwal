package ethwal

import (
	"context"

	"github.com/0xsequence/ethwal/storage"
)

const indexDir = ".idx"

type writerWithFilter[T any] struct {
	writer  Writer[T]
	indexes map[IndexName]Index[T]
	fs      storage.FS
}

var _ Writer[any] = (*writerWithFilter[any])(nil)

func NewWriterWithIndexBuilder[T any](writer Writer[T], indexes Indexes[T]) (Writer[T], error) {
	return &writerWithFilter[T]{indexes: indexes, writer: writer, fs: storage.NewPrefixWrapper(writer.FileSystem(), indexDir)}, nil
}

func (c *writerWithFilter[T]) FileSystem() storage.FS {
	return c.fs
}

func (c *writerWithFilter[T]) Write(ctx context.Context, block Block[T]) error {
	// update indexes first (idempotent)
	err := c.store(ctx, block)
	if err != nil {
		return err
	}

	// write block
	err = c.writer.Write(ctx, block)
	if err != nil {
		return err
	}
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

func (c *writerWithFilter[T]) store(ctx context.Context, block Block[T]) error {
	for _, index := range c.indexes {
		bmUpdate, err := index.Index(block)
		if err != nil {
			return err
		}

		if bmUpdate == nil {
			continue
		}

		err = index.Store(ctx, c.fs, bmUpdate)
		if err != nil {
			return err
		}
	}
	return nil
}
