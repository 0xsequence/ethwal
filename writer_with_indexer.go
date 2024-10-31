package ethwal

import (
	"context"
	"fmt"
	"log"

	"github.com/0xsequence/ethwal/storage"
)

type writerWithIndexer[T any] struct {
	writer Writer[T]

	indexer *Indexer[T]
}

var _ Writer[any] = (*writerWithIndexer[any])(nil)

func NewWriterWithIndexer[T any](writer Writer[T], indexer *Indexer[T]) (Writer[T], error) {
	if writer.BlockNum() > indexer.BlockNum() {
		// todo: implement a way to catch up indexer with writer
		// this should never happen if the writer with indexer is used
		return nil, fmt.Errorf("writer is ahead of indexer, can't catch up")
	}

	opts := writer.Options()
	wrappedPolicy := NewWrappedRollPolicy(opts.FileRollPolicy, func(ctx context.Context) {
		err := indexer.Flush(ctx)
		if err != nil {
			log.Default().Println("failed to flush index", "err", err)
		}
	})
	opts.FileRollPolicy = wrappedPolicy
	writer.SetOptions(opts)

	return &writerWithIndexer[T]{indexer: indexer, writer: writer}, nil
}

func (c *writerWithIndexer[T]) FileSystem() storage.FS {
	return c.writer.FileSystem()
}

func (c *writerWithIndexer[T]) Write(ctx context.Context, block Block[T]) error {
	// update indexes first (idempotent)
	err := c.index(ctx, block)
	if err != nil {
		return err
	}

	// write block, noop if block already written
	err = c.writer.Write(ctx, block)
	if err != nil {
		return err
	}
	return nil
}

func (c *writerWithIndexer[T]) Close(ctx context.Context) error {
	err := c.indexer.Close(ctx)
	if err != nil {
		return err
	}
	return c.writer.Close(ctx)
}

func (c *writerWithIndexer[T]) BlockNum() uint64 {
	return min(c.writer.BlockNum(), c.indexer.BlockNum())
}

func (c *writerWithIndexer[T]) RollFile(ctx context.Context) error {
	err := c.indexer.Flush(ctx)
	if err != nil {
		return err
	}
	return c.writer.RollFile(ctx)
}

func (c *writerWithIndexer[T]) Options() Options {
	return c.writer.Options()
}

func (c *writerWithIndexer[T]) SetOptions(options Options) {
	c.writer.SetOptions(options)
}

func (c *writerWithIndexer[T]) index(ctx context.Context, block Block[T]) error {
	return c.indexer.Index(ctx, block)
}
