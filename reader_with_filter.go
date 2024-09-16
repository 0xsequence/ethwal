package ethwal

import (
	"context"
	"io"
	"reflect"
)

type chainLensReader[T any] struct {
	reader   Reader[T]
	filter   Filter
	iterator FilterIterator
}

var _ Reader[any] = (*chainLensReader[any])(nil)

func NewReaderWithFilter[T any](reader Reader[T], filter Filter) (Reader[T], error) {
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
	// TODO: INCOMPLETE
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

	indexes := make([]uint16, 0)
	blockNum, i := c.iterator.Next()
	indexes = append(indexes, i)
	for b, _ := c.iterator.Peek(); c.iterator.HasNext() && b == blockNum; {
		_, i = c.iterator.Next()
		indexes = append(indexes, i)
	}

	err := c.reader.Seek(ctx, blockNum)
	if err != nil {
		return Block[T]{}, err
	}

	block, err := c.reader.Read(ctx)
	if err != nil {
		return Block[T]{}, err
	}

	if dType := reflect.TypeOf(block.Data); dType.Kind() == reflect.Slice || dType.Kind() == reflect.Array {
		newData := reflect.Indirect(reflect.New(dType))
		for _, i := range indexes {
			newData = reflect.Append(newData, reflect.ValueOf(block.Data).Index(int(i)))
		}
		block.Data = newData.Interface().(T)
	}

	return block, nil
}

func (c *chainLensReader[T]) Close() error {
	return c.reader.Close()
}
