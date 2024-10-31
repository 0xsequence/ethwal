package ethwal

import (
	"context"

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

type filterBuilder[T any] struct {
	indexes map[IndexName]Index[T]
	fs      storage.FS
}

func NewFilterBuilder[T any](indexes Indexes[T], fs storage.FS) (FilterBuilder, error) {
	return &filterBuilder[T]{
		indexes: indexes,
		fs:      fs,
	}, nil
}

type filter struct {
	resultSet *roaring64.Bitmap
}

func (c *filter) Eval() FilterIterator {
	if c.resultSet == nil {
		c.resultSet = roaring64.New()
	}

	return newFilterIterator(c.resultSet.Clone())
}

func (c *filterBuilder[T]) And(filters ...Filter) Filter {
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
			bmap.And(iter.Bitmap())
		}
	}
	return &filter{
		resultSet: bmap,
	}
}

func (c *filterBuilder[T]) Or(filters ...Filter) Filter {
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

	return &filter{
		resultSet: bmap,
	}
}

func (c *filterBuilder[T]) Eq(index string, key string) Filter {
	// fetch the IndexBlock and store it in the result set
	index_ := IndexName(index).Normalize()
	idx, ok := c.indexes[index_]
	if !ok {
		return &noOpFilter{}
	}

	// TODO: what should the context be?
	// TODO: lazy... do that on Eval?
	bitmap, err := idx.Fetch(context.Background(), c.fs, IndexedValue(key))
	if err != nil {
		return &noOpFilter{}
	}

	return &filter{
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
