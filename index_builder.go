package ethwal

import (
	"context"
	"sync"

	"github.com/0xsequence/ethwal/storage"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

type IndexBuilder[T any] struct {
	mu         sync.Mutex
	indexes    map[IndexName]Index[T]
	indexMaps  map[IndexName]map[IndexValue]*roaring64.Bitmap
	fs         storage.FS
	rollPolicy FileRollPolicy
}

func NewIndexBuilder[T any](indexes Indexes[T], fs storage.FS, rollPolicy FileRollPolicy) (*IndexBuilder[T], error) {
	indexMaps := make(map[IndexName]map[IndexValue]*roaring64.Bitmap)
	for _, index := range indexes {
		indexMaps[index.name] = make(map[IndexValue]*roaring64.Bitmap)
	}
	return &IndexBuilder[T]{indexes: indexes, indexMaps: indexMaps, fs: fs, rollPolicy: rollPolicy}, nil
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
			dataWritten, err := bm.MarshalBinary()
			if err != nil {
				continue
			}
			b.rollPolicy.onWrite(dataWritten)
		}
		b.mu.Unlock()
	}
	b.rollPolicy.onBlockProcessed(block.Number)

	if b.rollPolicy.ShouldRoll() {
		return b.Flush(ctx)
	}

	return nil
}

func (b *IndexBuilder[T]) Close(ctx context.Context) error {
	return b.Flush(ctx)
}

func (b *IndexBuilder[T]) Flush(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	defer b.rollPolicy.Reset()

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
