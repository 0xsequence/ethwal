package ethwal

import (
	"cmp"
	"context"
	"fmt"
	"path"
	"reflect"

	"github.com/0xsequence/ethwal/storage"
	"github.com/0xsequence/ethwal/storage/local"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

// Filter is an interface that defines the methods to filter blocks
// based on the index data.
type Filter[T any] interface {
	// Filter blocks inner data based on the filter criteria.
	Filter(block Block[T]) Block[T]

	// IndexIterator returns the iterator for the filter.
	IndexIterator(ctx context.Context) *IndexIterator

	bitmap(block Block[T]) *roaring64.Bitmap
}

type FilterBuilder[T any] interface {
	And(filters ...Filter[T]) Filter[T]
	Or(filters ...Filter[T]) Filter[T]
	Eq(index string, key string) Filter[T]
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
	indexes map[IndexName]Index[T]
	fs      storage.FS
}

func NewFilterBuilder[T any](opt FilterBuilderOptions[T]) (FilterBuilder[T], error) {
	// apply default options on uninitialized fields
	opt = opt.WithDefaults()

	// mount indexes directory
	fs := storage.NewPrefixWrapper(opt.FileSystem, fmt.Sprintf("%s/", path.Join(opt.Dataset.FullPath(), IndexesDirectory)))

	return &filterBuilder[T]{
		indexes: opt.Indexes,
		fs:      fs,
	}, nil
}

type filter[T any] struct {
	blockBitmap         func(ctx context.Context) *roaring64.Bitmap
	dataIndexBitmapFunc func(block Block[T]) *roaring64.Bitmap
}

func (c *filter[T]) IndexIterator(ctx context.Context) *IndexIterator {
	if c.blockBitmap == nil {
		c.blockBitmap = func(ctx context.Context) *roaring64.Bitmap {
			return roaring64.New()
		}
	}
	return NewIndexIterator(c.blockBitmap(ctx))
}

func (c *filter[T]) Filter(block Block[T]) Block[T] {
	dataIndexesBitmap := c.dataIndexBitmapFunc(block)
	dataIndexes := dataIndexesBitmap.ToArray()
	if len(dataIndexes) == 1 && dataIndexes[0] == IndexAllDataIndexes {
		return block
	}

	if dType := reflect.TypeOf(block.Data); dType.Kind() == reflect.Slice || dType.Kind() == reflect.Array {
		newData := reflect.Indirect(reflect.New(dType))
		for _, dataIndex := range dataIndexes {
			newData = reflect.Append(newData, reflect.ValueOf(block.Data).Index(int(dataIndex)))
		}
		block.Data = newData.Interface().(T)
	}
	return block
}

func (c *filter[T]) bitmap(block Block[T]) *roaring64.Bitmap {
	if c.dataIndexBitmapFunc == nil {
		return roaring64.New()
	}
	return c.dataIndexBitmapFunc(block)
}

func (c *filterBuilder[T]) And(conds ...Filter[T]) Filter[T] {
	return &filter[T]{
		blockBitmap: func(ctx context.Context) *roaring64.Bitmap {
			var bmap *roaring64.Bitmap
			for _, cond := range conds {
				if cond == nil {
					continue
				}

				iter := cond.IndexIterator(ctx)
				if bmap == nil {
					bmap = iter.Bitmap().Clone()
				} else {
					bmap.And(iter.Bitmap())
				}
			}
			return bmap
		},
		dataIndexBitmapFunc: func(block Block[T]) *roaring64.Bitmap {
			var bmap *roaring64.Bitmap
			for _, cond := range conds {
				condBitmap := cond.bitmap(block)
				if bmap == nil {
					bmap = condBitmap.Clone()
				} else {
					bmap.And(condBitmap)
				}
			}
			return bmap
		},
	}
}

func (c *filterBuilder[T]) Or(conds ...Filter[T]) Filter[T] {
	return &filter[T]{
		blockBitmap: func(ctx context.Context) *roaring64.Bitmap {
			var bmap *roaring64.Bitmap
			for _, cond := range conds {
				if cond == nil {
					continue
				}

				iter := cond.IndexIterator(ctx)
				if bmap == nil {
					bmap = iter.Bitmap().Clone()
				} else {
					bmap.Or(iter.Bitmap())
				}
			}
			return bmap
		},
		dataIndexBitmapFunc: func(block Block[T]) *roaring64.Bitmap {
			var bmap *roaring64.Bitmap
			for _, cond := range conds {
				condBitmap := cond.bitmap(block)
				if bmap == nil {
					bmap = condBitmap.Clone()
				} else {
					bmap.Or(condBitmap)
				}
			}
			return bmap
		},
	}
}

func (c *filterBuilder[T]) Eq(index string, key string) Filter[T] {
	return &filter[T]{
		blockBitmap: func(ctx context.Context) *roaring64.Bitmap {
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
		dataIndexBitmapFunc: func(block Block[T]) *roaring64.Bitmap {
			index_ := IndexName(index).Normalize()
			idx, ok := c.indexes[index_]
			if !ok {
				return roaring64.New()
			}

			indexUpdate, _ := idx.IndexBlock(context.Background(), nil, block)
			bitmap, ok := indexUpdate.DataIndexBitmap[IndexedValue(key)]
			if !ok {
				return roaring64.New()
			}
			return bitmap
		},
	}
}
