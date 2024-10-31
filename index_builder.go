package ethwal

import (
	"context"
	"fmt"
	"sync"

	"github.com/0xsequence/ethwal/storage"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

type IndexBuilder[T any] struct {
	mu           sync.Mutex
	indexes      map[IndexName]Index[T]
	indexUpdates map[IndexName]*IndexUpdate
	fs           storage.FS
}

func NewIndexBuilder[T any](ctx context.Context, indexes Indexes[T], fs storage.FS) (*IndexBuilder[T], error) {
	indexMaps := make(map[IndexName]*IndexUpdate)
	for _, index := range indexes {
		lastBlockNum, err := index.LastBlockNumIndexed(ctx, fs)
		if err != nil {
			return nil, fmt.Errorf("IndexBuilder.NewIndexBuilder: failed to get last block number indexed for %s: %w", index.Name(), err)
		}

		indexMaps[index.name] = &IndexUpdate{Data: make(map[IndexedValue]*roaring64.Bitmap), LastBlockNum: lastBlockNum}
	}
	return &IndexBuilder[T]{indexes: indexes, indexUpdates: indexMaps, fs: fs}, nil
}

func (b *IndexBuilder[T]) Index(ctx context.Context, block Block[T]) error {
	for _, index := range b.indexes {
		bmUpdate, err := index.IndexBlock(ctx, b.fs, block)
		if err != nil {
			return err
		}
		if bmUpdate == nil {
			continue
		}

		b.mu.Lock()
		updateBatch := b.indexUpdates[index.name]
		updateBatch.Merge(bmUpdate)
		b.indexUpdates[index.name] = updateBatch
		b.mu.Unlock()
	}

	return nil
}

func (b *IndexBuilder[T]) Flush(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for name, indexUpdate := range b.indexUpdates {
		idx, ok := b.indexes[name]
		if !ok {
			continue
		}

		err := idx.Store(ctx, b.fs, indexUpdate)
		if err != nil {
			return err
		}
	}

	// clear indexUpdates
	for _, index := range b.indexes {
		b.indexUpdates[index.name].Data = make(map[IndexedValue]*roaring64.Bitmap)
	}
	return nil
}

// LastIndexedBlockNum returns the lowest block number indexed by all indexes. If no blocks have been indexed, it returns 0.
// This is useful for determining the starting block number for a new IndexBuilder.
func (b *IndexBuilder[T]) LastIndexedBlockNum(ctx context.Context) (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	var lowestBlockNum *uint64
	for _, index := range b.indexes {
		numBlocksIndexed, err := index.LastBlockNumIndexed(ctx, b.fs)
		if err != nil {
			return 0, fmt.Errorf("IndexBuilder.LastIndexedBlockNum: failed to get number of blocks indexed: %w", err)
		}
		if lowestBlockNum == nil || numBlocksIndexed < *lowestBlockNum {
			lowestBlockNum = &numBlocksIndexed
		}
	}

	if lowestBlockNum == nil {
		return 0, nil
	}

	return *lowestBlockNum, nil
}

func (b *IndexBuilder[T]) Close(ctx context.Context) error {
	return b.Flush(ctx)
}
