package ethwal

import (
	"context"
	"log"

	"github.com/0xsequence/ethwal/storage"
)

const indexDir = ".idx/"

type writerWithFilter[T any] struct {
	writer       Writer[T]
	indexBuilder *IndexBuilder[T]
	fs           storage.FS
}

var _ Writer[any] = (*writerWithFilter[any])(nil)

func NewWriterWithIndexBuilder[T any](ctx context.Context, writer Writer[T], indexes Indexes[T]) (Writer[T], error) {
	fs := storage.NewPrefixWrapper(writer.FileSystem(), indexDir)
	indexBuilder, err := NewIndexBuilder[T](ctx, indexes, fs)
	opts := writer.Options()
	wrappedPolicy := NewWrappedRollPolicy(opts.FileRollPolicy, func(ctx context.Context) {
		err := indexBuilder.Flush(ctx)
		if err != nil {
			log.Default().Println("failed to flush index", "err", err)
		}
	})
	opts.FileRollPolicy = wrappedPolicy
	writer.SetOptions(opts)

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

func (c *writerWithFilter[T]) Options() Options {
	return c.writer.Options()
}

func (c *writerWithFilter[T]) SetOptions(options Options) {
	c.writer.SetOptions(options)
}

func (c *writerWithFilter[T]) store(ctx context.Context, block Block[T]) error {
	return c.indexBuilder.Index(ctx, block)
}
