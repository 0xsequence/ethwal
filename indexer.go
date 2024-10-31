package ethwal

import (
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/c2h5oh/datasize"
	"golang.org/x/sync/errgroup"
)

type Indexer[T any] struct {
	mu           sync.Mutex
	indexes      map[IndexName]Index[T]
	indexUpdates map[IndexName]*IndexUpdate
}

func NewIndexer[T any](ctx context.Context, indexes Indexes[T]) (*Indexer[T], error) {
	indexMaps := make(map[IndexName]*IndexUpdate)
	for _, index := range indexes {
		lastBlockNum, err := index.LastBlockNumIndexed(ctx)
		if err != nil {
			return nil, fmt.Errorf("Indexer.NewIndexer: failed to get last block number indexed for %s: %w", index.Name(), err)
		}

		indexMaps[index.name] = &IndexUpdate{Data: make(map[IndexedValue]*roaring64.Bitmap), LastBlockNum: lastBlockNum}
	}
	return &Indexer[T]{indexes: indexes, indexUpdates: indexMaps}, nil
}

func (b *Indexer[T]) Index(ctx context.Context, block Block[T]) error {
	for _, index := range b.indexes {
		bmUpdate, err := index.IndexBlock(ctx, block)
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

func (b *Indexer[T]) EstimatedBatchSize() datasize.ByteSize {
	b.mu.Lock()
	defer b.mu.Unlock()

	var size datasize.ByteSize = 0
	for _, indexUpdate := range b.indexUpdates {
		for _, bm := range indexUpdate.Data {
			size += datasize.ByteSize(bm.GetSizeInBytes())
		}
	}
	return size
}

func (b *Indexer[T]) Flush(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	errGrp, gCtx := errgroup.WithContext(ctx)

	for name, indexUpdate := range b.indexUpdates {
		idx, ok := b.indexes[name]
		if !ok {
			continue
		}

		errGrp.Go(func() error {
			err := idx.Store(gCtx, indexUpdate)
			if err != nil {
				return err
			}
			return nil
		})
	}

	err := errGrp.Wait()
	if err != nil {
		return fmt.Errorf("Indexer.Flush: failed to flush indexes: %w", err)
	}

	// clear indexUpdates
	for _, index := range b.indexes {
		b.indexUpdates[index.name].Data = make(map[IndexedValue]*roaring64.Bitmap)
	}
	return nil
}

// BlockNum returns the lowest block number indexed by all indexes. If no blocks have been indexed, it returns 0.
// This is useful for determining the starting block number for a new Indexer.
func (b *Indexer[T]) BlockNum() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()

	var lowestBlockNum uint64 = math.MaxUint64
	for _, indexUpdate := range b.indexUpdates {
		if indexUpdate.LastBlockNum < lowestBlockNum {
			lowestBlockNum = indexUpdate.LastBlockNum
		}
	}

	if lowestBlockNum == math.MaxUint64 {
		return 0
	}
	return lowestBlockNum
}

func (b *Indexer[T]) Close(ctx context.Context) error {
	return b.Flush(ctx)
}
