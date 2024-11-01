package ethwal

import (
	"cmp"
	"context"
	"fmt"
	"path"

	"github.com/0xsequence/ethwal/storage"
	"github.com/0xsequence/ethwal/storage/local"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

type Filter interface {
	Eval(ctx context.Context) FilterIterator
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

type FilterBuilderOptions[T any] struct {
	Dataset    Dataset
	FileSystem storage.FS

	Indexes Indexes[T]
}

func (o FilterBuilderOptions[T]) WithDefaults() FilterBuilderOptions[T] {
	o.FileSystem = cmp.Or(o.FileSystem, storage.FS(local.NewLocalFS("")))
	return o
}

type filterBuilder[T any] struct {
	ctx context.Context

	indexes map[IndexName]Index[T]
	fs      storage.FS
}

func NewFilterBuilder[T any](opt FilterBuilderOptions[T]) (FilterBuilder, error) {
	// apply default options on uninitialized fields
	opt = opt.WithDefaults()

	// mount indexes directory
	fs := storage.NewPrefixWrapper(opt.FileSystem, fmt.Sprintf("%s/", path.Join(opt.Dataset.FullPath(), IndexesDirectory)))

	return &filterBuilder[T]{
		indexes: opt.Indexes,
		fs:      fs,
	}, nil
}

type filter struct {
	resultSet func(ctx context.Context) *roaring64.Bitmap
}

func (c *filter) Eval(ctx context.Context) FilterIterator {
	if c.resultSet == nil {
		c.resultSet = func(ctx context.Context) *roaring64.Bitmap {
			return roaring64.New()
		}
	}
	return newFilterIterator(c.resultSet(ctx))
}

func (c *filterBuilder[T]) And(filters ...Filter) Filter {
	return &filter{
		resultSet: func(ctx context.Context) *roaring64.Bitmap {
			var bmap *roaring64.Bitmap
			for _, filter := range filters {
				if filter == nil {
					continue
				}

				iter := filter.Eval(ctx)
				if bmap == nil {
					bmap = iter.Bitmap().Clone()
				} else {
					bmap.And(iter.Bitmap())
				}
			}
			return bmap
		},
	}
}

func (c *filterBuilder[T]) Or(filters ...Filter) Filter {
	return &filter{
		resultSet: func(ctx context.Context) *roaring64.Bitmap {
			var bmap *roaring64.Bitmap
			for _, filter := range filters {
				if filter == nil {
					continue
				}

				iter := filter.Eval(ctx)
				if bmap == nil {
					bmap = iter.Bitmap().Clone()
				} else {
					bmap.Or(iter.Bitmap())
				}
			}
			return bmap
		},
	}
}

func (c *filterBuilder[T]) Eq(index string, key string) Filter {

	return &filter{
		resultSet: func(ctx context.Context) *roaring64.Bitmap {
			// fetch the index file and include it in the result set
			index_ := IndexName(index).Normalize()
			idx, ok := c.indexes[index_]
			if !ok {
				return roaring64.New()
			}

			bitmap, err := idx.Fetch(ctx, c.fs, IndexedValue(key))
			if err != nil {
				return roaring64.New()
			}
			return bitmap
		},
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
