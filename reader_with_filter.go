package ethwal

import (
	"context"
	"io"
)

type readerWithFilter[T any] struct {
	lastBlockNum uint64
	reader       Reader[T]
	filter       Filter[T]
	iterator     *IndexIterator
}

var _ Reader[any] = (*readerWithFilter[any])(nil)

func NewReaderWithFilter[T any](reader Reader[T], filter Filter[T]) (Reader[T], error) {
	return &readerWithFilter[T]{
		reader: reader,
		filter: filter,
	}, nil
}

func (c *readerWithFilter[T]) FileNum() int {
	return c.reader.FileNum()
}

func (c *readerWithFilter[T]) FileIndex() *FileIndex {
	return c.reader.FileIndex()
}

func (c *readerWithFilter[T]) Seek(ctx context.Context, blockNum uint64) error {
	iter := c.filter.IndexIterator(ctx)
	for iter.HasNext() {
		nextBlock := iter.Peek()
		if nextBlock >= blockNum {
			break
		}
		iter.Next()
	}

	c.iterator = iter
	return nil
}

func (c *readerWithFilter[T]) BlockNum() uint64 {
	return c.lastBlockNum
}

func (c *readerWithFilter[T]) Read(ctx context.Context) (Block[T], error) {
	// Lazy init iterator
	if c.iterator == nil {
		c.iterator = c.filter.IndexIterator(ctx)
	}

	// Check if there are no more blocks to read
	if !c.iterator.HasNext() {
		return Block[T]{}, io.EOF
	}

	// Collect all data indexes for the block
	blockNum := c.iterator.Next()

	// Seek to the block
	err := c.reader.Seek(ctx, blockNum)
	if err != nil {
		return Block[T]{}, err
	}

	block, err := c.reader.Read(ctx)
	if err != nil {
		return Block[T]{}, err
	}

	// Filter
	block = c.filter.Filter(block)

	c.lastBlockNum = blockNum
	return block, nil
}

func (c *readerWithFilter[T]) Close() error {
	return c.reader.Close()
}
