package ethwal

import (
	"context"
	"io"
	"reflect"
)

type readerWithFilter[T any] struct {
	lastBlockNum uint64
	reader       Reader[T]
	filter       Filter
	iterator     FilterIterator
}

var _ Reader[any] = (*readerWithFilter[any])(nil)

func NewReaderWithFilter[T any](reader Reader[T], filter Filter) (Reader[T], error) {
	return &readerWithFilter[T]{
		reader:   reader,
		filter:   filter,
		iterator: filter.Eval(),
	}, nil
}

func (c *readerWithFilter[T]) FilesNum() int {
	return c.reader.FilesNum()
}

func (c *readerWithFilter[T]) Seek(ctx context.Context, blockNum uint64) error {
	iter := c.filter.Eval()
	for iter.HasNext() {
		nextBlock, _ := iter.Peek()
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
	if !c.iterator.HasNext() {
		return Block[T]{}, io.EOF
	}

	// Collect all data indexes for the block
	blockNum, dataIndex := c.iterator.Next()
	dataIndexes := []uint16{dataIndex}

	doFilter := dataIndex != IndexAllDataIndexes
	for c.iterator.HasNext() {
		nextBlockNum, nextDataIndex := c.iterator.Peek()
		if blockNum != nextBlockNum {
			break
		}

		_, _ = c.iterator.Next()
		dataIndexes = append(dataIndexes, nextDataIndex)
	}

	// Seek to the block
	err := c.reader.Seek(ctx, blockNum)
	if err != nil {
		return Block[T]{}, err
	}

	block, err := c.reader.Read(ctx)
	if err != nil {
		return Block[T]{}, err
	}

	// Filter the block data
	if dType := reflect.TypeOf(block.Data); doFilter && (dType.Kind() == reflect.Slice || dType.Kind() == reflect.Array) {
		newData := reflect.Indirect(reflect.New(dType))
		for _, dataIndex := range dataIndexes {
			newData = reflect.Append(newData, reflect.ValueOf(block.Data).Index(int(dataIndex)))
		}
		block.Data = newData.Interface().(T)
	}

	c.lastBlockNum = blockNum
	return block, nil
}

func (c *readerWithFilter[T]) Close() error {
	return c.reader.Close()
}
