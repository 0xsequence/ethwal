package ethwal

import (
	"context"
	"io"
)

type chainLensReader[T any] struct {
	reader   Reader[T]
	filter   Filter
	iterator FilterIterator
}

var _ Reader[any] = (*chainLensReader[any])(nil)

func NewChainLensReader[T any](reader Reader[T], filter Filter) (Reader[T], error) {
	return &chainLensReader[T]{
		reader:   reader,
		filter:   filter,
		iterator: filter.Eval(),
	}, nil
}

func (c *chainLensReader[T]) FilesNum() int {
	return c.reader.FilesNum()
}

func (c *chainLensReader[T]) Seek(ctx context.Context, blockNum uint64) error {
	// TODO: how should seek function?
	return c.reader.Seek(ctx, blockNum)
}

func (c *chainLensReader[T]) BlockNum() uint64 {
	// TODO: INCOMPLETE
	return c.reader.BlockNum()
}

func (c *chainLensReader[T]) Read(ctx context.Context) (Block[T], error) {
	if !c.iterator.HasNext() {
		return Block[T]{}, io.EOF
	}

	// TODO: decide about the index what to do??
	blockNum, _ := c.iterator.Next()
	err := c.reader.Seek(ctx, blockNum)
	if err != nil {
		return Block[T]{}, err
	}

	return c.reader.Read(ctx)
}

func (c *chainLensReader[T]) Close() error {
	return c.reader.Close()
}
