package ethwal

import (
	"context"

	"github.com/0xsequence/ethwal/storage"
)

const indexDir = ".idx"
const defaultBlockRollInterval = 100

type writerWithFilter[T any] struct {
	writer       Writer[T]
	indexBuilder *IndexBuilder[T]
	fs           storage.FS
}

var _ Writer[any] = (*writerWithFilter[any])(nil)

func NewWriterWithIndexBuilder[T any](writer Writer[T], indexes Indexes[T]) (Writer[T], error) {
	fs := storage.NewPrefixWrapper(writer.FileSystem(), indexDir)
	indexBuilder, err := NewIndexBuilder[T](indexes, fs, NewLastBlockNumberRollPolicy(defaultBlockRollInterval))
	if err != nil {
		return nil, err
	}
	return &writerWithFilter[T]{indexBuilder: indexBuilder, writer: writer, fs: fs}, nil
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
	err := c.indexBuilder.Close(ctx)
	if err != nil {
		return err
	}
	return c.writer.Close(ctx)
}

func (c *writerWithFilter[T]) BlockNum() uint64 {
	return c.writer.BlockNum()
}

func (c *writerWithFilter[T]) RollFile(ctx context.Context) error {
	err := c.indexBuilder.Flush(ctx)
	if err != nil {
		return err
	}
	return c.writer.RollFile(ctx)
}

func (c *writerWithFilter[T]) store(ctx context.Context, block Block[T]) error {
	return c.indexBuilder.Index(ctx, block)
}
