package ethwal

import (
	"context"
	"fmt"
	"sync"

	"github.com/0xsequence/ethwal/storage"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

type IndexBuilder[T any] struct {
	mu             sync.Mutex
	indexes        map[IndexName]Index[T]
	indexMaps      map[IndexName]map[IndexValue]*roaring64.Bitmap
	indexMaxBlocks map[IndexName]uint64
	fs             storage.FS
}

func NewIndexBuilder[T any](indexes Indexes[T], fs storage.FS) (*IndexBuilder[T], error) {
	indexMaps := make(map[IndexName]map[IndexValue]*roaring64.Bitmap)
	indexMaxBlocks := make(map[IndexName]uint64)
	for _, index := range indexes {
		indexMaps[index.name] = make(map[IndexValue]*roaring64.Bitmap)
		indexMaxBlocks[index.name] = 0
	}
	return &IndexBuilder[T]{indexes: indexes, indexMaps: indexMaps, indexMaxBlocks: indexMaxBlocks, fs: fs}, nil
}

func (b *IndexBuilder[T]) Index(ctx context.Context, block Block[T]) error {
	for _, index := range b.indexes {
		bmUpdate, err := index.Index(ctx, b.fs, block)
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

		if b.indexMaxBlocks[index.name] < block.Number {
			b.indexMaxBlocks[index.name] = block.Number
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

		err := idx.Store(ctx, b.fs, indexMap, b.indexMaxBlocks[name])
		if err != nil {
			return err
		}
	}

	// clear indexMaps
	b.indexMaps = make(map[IndexName]map[IndexValue]*roaring64.Bitmap)
	b.indexMaxBlocks = make(map[IndexName]uint64)
	for _, index := range b.indexes {
		b.indexMaps[index.name] = make(map[IndexValue]*roaring64.Bitmap)
		b.indexMaxBlocks[index.name] = 0
	}

	return nil
}

func (b *IndexBuilder[T]) GetLowestIndexedBlockNum(ctx context.Context) (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	lowestBlockNum := uint64(0)
	for _, index := range b.indexes {
		numBlocksIndexed, err := index.NumBlocksIndexed(ctx, b.fs)
		if err != nil {
			return 0, fmt.Errorf("IndexBuilder.GetLowestIndexedBlockNum: failed to get number of blocks indexed: %w", err)
		}
		if lowestBlockNum == 0 || numBlocksIndexed < lowestBlockNum {
			lowestBlockNum = numBlocksIndexed
		}
	}

	return lowestBlockNum, nil
}
