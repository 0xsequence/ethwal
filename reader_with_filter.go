package ethwal

import (
	"context"
	"io"
	"math"
	"reflect"
)

type chainLensReader[T any] struct {
	lastBlockNum uint64
	reader       Reader[T]
	filter       Filter
	iterator     FilterIterator
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
	// seek the iterator to the closes block to the blockNum
	iter := c.filter.Eval()
	currBlock, _ := iter.Next()
	currDist := math.Abs(float64(blockNum - currBlock))
	for iter.HasNext() {
		nextBlock, _ := iter.Peek()
		nextDist := math.Abs(float64(blockNum - nextBlock))
		if nextDist > currDist {
			break
		}
		// since next is closer, move to next
		iter.Next()
		currDist = nextDist
	}

	// set the iterator to the desired block num
	c.iterator = iter
	return nil
}

func (c *chainLensReader[T]) BlockNum() uint64 {
	return c.lastBlockNum
}

func (c *chainLensReader[T]) Read(ctx context.Context) (Block[T], error) {
	if !c.iterator.HasNext() {
		return Block[T]{}, io.EOF
	}

	dataIndexes := make([]uint16, 0)
	blockNum, dataIndex := c.iterator.Next()
	dataIndexes = append(dataIndexes, dataIndex)
	for b, dataIndex := c.iterator.Peek(); c.iterator.HasNext() && b == blockNum; {
		b, dataIndex = c.iterator.Peek()
		if b != blockNum {
			break
		}

		b, dataIndex = c.iterator.Next()
		dataIndexes = append(dataIndexes, dataIndex)
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
		for _, dataIndex := range dataIndexes {
			newData = reflect.Append(newData, reflect.ValueOf(block.Data).Index(int(dataIndex)))
		}
		block.Data = newData.Interface().(T)
	}

	c.lastBlockNum = blockNum
	return block, nil
}

func (c *chainLensReader[T]) Close() error {
	return c.reader.Close()
}
