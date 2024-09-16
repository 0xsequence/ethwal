package ethwal

import (
	"context"
	"fmt"

	"github.com/0xsequence/ethwal/storage"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

type Filter interface {
	Eval() FilterIterator
}

type FilterIterator interface {
	HasNext() bool
	Next() (uint64, uint16)
	Peek() (uint64, uint16)
	Bitmap() *roaring64.Bitmap
}

type FilterBuilder interface {
	And(filters ...Filter) Filter
	Or(filters ...Filter) Filter
	Eq(index string, key string) Filter
}

func NewIndexesFilterBuilder[T any](indexes Indexes[T], fs storage.FS) (FilterBuilder, error) {
	indexMap := make(map[IndexName]Index[T])
	for name, indexFunc := range indexes {
		idx := &index[T]{
			name:      name.Normalize(),
			indexFunc: indexFunc,
			fs:        fs,
		}
		indexMap[name] = idx
	}
	return &chainLensFilterBuilder[T]{
		indexes: indexMap,
	}, nil
}

type chainLensFilterBuilder[T any] struct {
	indexes map[IndexName]Index[T]
}

type chainLensFilter struct {
	resultSet *roaring64.Bitmap
}

func (c *chainLensFilter) Eval() FilterIterator {
	if c.resultSet == nil {
		c.resultSet = roaring64.New()
	}

	return newFilterIterator(c.resultSet.Clone())
}

func (c *chainLensFilterBuilder[T]) And(filters ...Filter) Filter {
	var bmap *roaring64.Bitmap
	for _, filter := range filters {
		if filter == nil {
			continue
		}
		if _, ok := filter.(*noOpFilter); ok {
			continue
		}
		iter := filter.Eval()
		if bmap == nil {
			bmap = iter.Bitmap().Clone()
			fmt.Println("first bmap", bmap.GetCardinality())
		} else {
			fmt.Println("iter", iter.Bitmap().GetCardinality())
			bmap.And(iter.Bitmap())
		}
	}
	return &chainLensFilter{
		resultSet: bmap,
	}
}

func (c *chainLensFilterBuilder[T]) Or(filters ...Filter) Filter {
	var bmap *roaring64.Bitmap
	for _, filter := range filters {
		if filter == nil {
			continue
		}
		if _, ok := filter.(*noOpFilter); ok {
			continue
		}
		iter := filter.Eval()
		if bmap == nil {
			bmap = iter.Bitmap().Clone()
		} else {
			bmap.Or(iter.Bitmap())
		}
	}

	return &chainLensFilter{
		resultSet: bmap,
	}
}

func (c *chainLensFilterBuilder[T]) Eq(index string, key string) Filter {
	// fetch the index and store it in the result set
	index_ := IndexName(index).Normalize()
	idx, ok := c.indexes[index_]
	if !ok {
		return &noOpFilter{}
	}

	// TODO: what should the context be?
	bitmap, err := idx.Fetch(context.Background(), key)
	if err != nil {
		return &noOpFilter{}
	}

	return &chainLensFilter{
		resultSet: bitmap,
	}
}

type noOpFilter struct{}

func (n *noOpFilter) Eval() FilterIterator {
	bmap := roaring64.New()
	return &filterIterator{
		iter:   bmap.Iterator(),
		bitmap: bmap,
	}
}

type filterIterator struct {
	iter   roaring64.IntPeekable64
	bitmap *roaring64.Bitmap
}

func newFilterIterator(bmap *roaring64.Bitmap) FilterIterator {
	return &filterIterator{
		iter:   bmap.Iterator(),
		bitmap: bmap,
	}
}

func (f *filterIterator) HasNext() bool {
	return f.iter.HasNext()
}

func (f *filterIterator) Next() (uint64, uint16) {
	// TODO: how to handle if there's no next?
	val := f.iter.Next()
	return IndexCompoundID(val).Split()
}

func (f *filterIterator) Peek() (uint64, uint16) {
	val := f.iter.PeekNext()
	return IndexCompoundID(val).Split()
}

func (f *filterIterator) Bitmap() *roaring64.Bitmap {
	return f.bitmap
}
