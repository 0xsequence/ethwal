package ethwal

import (
	"context"
	"sync"

	"github.com/0xsequence/ethwal/storage"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

type IndexBuilder[T any] struct {
	mu        sync.Mutex
	indexes   map[IndexName]Index[T]
	indexMaps map[IndexName]map[IndexValue]*roaring64.Bitmap
	fs        storage.FS
}

func NewIndexBuilder[T any](indexes Indexes[T], fs storage.FS) (*IndexBuilder[T], error) {
	indexMaps := make(map[IndexName]map[IndexValue]*roaring64.Bitmap)
	for _, index := range indexes {
		indexMaps[index.name] = make(map[IndexValue]*roaring64.Bitmap)
	}
	return &IndexBuilder[T]{indexes: indexes, indexMaps: indexMaps, fs: fs}, nil
}

func (b *IndexBuilder[T]) Index(ctx context.Context, block Block[T]) error {
	for _, index := range b.indexes {
		bmUpdate, err := index.Index(block)
		if err != nil {
			return err
		}

		if bmUpdate == nil {
			continue
		}

		b.mu.Lock()
		for indexValue, bm := range bmUpdate {
			if _, ok := b.indexMaps[index.name][indexValue]; !ok {
				b.indexMaps[index.name][indexValue] = roaring64.New()
			}
			b.indexMaps[index.name][indexValue].Or(bm)
			if err != nil {
				continue
			}
		}
		b.mu.Unlock()
	}

	return nil
}

func (b *IndexBuilder[T]) Close(ctx context.Context) error {
	return b.Flush(ctx)
}

func (b *IndexBuilder[T]) Flush(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for name, indexMap := range b.indexMaps {
		idx, ok := b.indexes[name]
		if !ok {
			continue
		}

		err := idx.Store(ctx, b.fs, indexMap)
		if err != nil {
			return err
		}
	}

	// clear indexMaps
	b.indexMaps = make(map[IndexName]map[IndexValue]*roaring64.Bitmap)
	for _, index := range b.indexes {
		b.indexMaps[index.name] = make(map[IndexValue]*roaring64.Bitmap)
	}
	return nil
}
