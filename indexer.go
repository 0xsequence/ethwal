package ethwal

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"path"
	"sync"

	"github.com/0xsequence/ethwal/storage"
	"github.com/0xsequence/ethwal/storage/local"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/c2h5oh/datasize"
	"golang.org/x/sync/errgroup"
)

const IndexesDirectory = ".indexes"

type IndexerOptions[T any] struct {
	Dataset    Dataset
	FileSystem storage.FS

	Indexes Indexes[T]
}

func (o IndexerOptions[T]) WithDefaults() IndexerOptions[T] {
	o.FileSystem = cmp.Or(o.FileSystem, storage.FS(local.NewLocalFS("")))
	return o
}

type Indexer[T any] struct {
	indexes      map[IndexName]Index[T]
	indexUpdates map[IndexName]*IndexUpdate
	fs           storage.FS

	mu sync.Mutex
}

func NewIndexer[T any](ctx context.Context, opt IndexerOptions[T]) (*Indexer[T], error) {
	// apply default options on uninitialized fields
	opt = opt.WithDefaults()

	// mount indexes directory
	fs := storage.NewPrefixWrapper(opt.FileSystem, fmt.Sprintf("%s/", path.Join(opt.Dataset.FullPath(), IndexesDirectory)))

	// populate indexUpdates with last block number indexed
	indexMaps := make(map[IndexName]*IndexUpdate)
	for _, index := range opt.Indexes {
		lastBlockNum, err := index.LastBlockNumIndexed(ctx, fs)
		if err != nil {
			return nil, fmt.Errorf("Indexer.NewIndexer: failed to get last block number indexed for %s: %w", index.Name(), err)
		}

		indexMaps[index.name] = &IndexUpdate{BlockBitmap: make(map[IndexedValue]*roaring64.Bitmap), LastBlockNum: lastBlockNum}
	}

	return &Indexer[T]{
		indexes:      opt.Indexes,
		indexUpdates: indexMaps,
		fs:           fs,
	}, nil
}

func (i *Indexer[T]) Index(ctx context.Context, block Block[T]) error {
	for _, index := range i.indexes {
		bmUpdate, err := index.IndexBlock(ctx, i.fs, block)
		if err != nil {
			return err
		}
		if bmUpdate == nil {
			continue
		}

		i.mu.Lock()
		updateBatch := i.indexUpdates[index.name]
		updateBatch.Merge(bmUpdate)
		i.indexUpdates[index.name] = updateBatch
		i.mu.Unlock()
	}

	return nil
}

func (i *Indexer[T]) EstimatedBatchSize() datasize.ByteSize {
	i.mu.Lock()
	defer i.mu.Unlock()

	var size datasize.ByteSize = 0
	for _, indexUpdate := range i.indexUpdates {
		for _, bm := range indexUpdate.BlockBitmap {
			size += datasize.ByteSize(bm.GetSizeInBytes())
		}
	}
	return size
}

func (i *Indexer[T]) Flush(ctx context.Context) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	errGrp, gCtx := errgroup.WithContext(ctx)

	for name, indexUpdate := range i.indexUpdates {
		idx, ok := i.indexes[name]
		if !ok {
			continue
		}

		errGrp.Go(func() error {
			err := idx.Store(gCtx, i.fs, indexUpdate)
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
	for _, index := range i.indexes {
		i.indexUpdates[index.name].BlockBitmap = make(map[IndexedValue]*roaring64.Bitmap)
	}
	return nil
}

// BlockNum returns the lowest block number indexed by all indexes. If no blocks have been indexed, it returns 0.
// This is useful for determining the starting block number for a new Indexer.
func (i *Indexer[T]) BlockNum() uint64 {
	i.mu.Lock()
	defer i.mu.Unlock()

	var lowestBlockNum uint64 = math.MaxUint64
	for _, indexUpdate := range i.indexUpdates {
		if indexUpdate.LastBlockNum < lowestBlockNum {
			lowestBlockNum = indexUpdate.LastBlockNum
		}
	}

	if lowestBlockNum == math.MaxUint64 {
		return 0
	}
	return lowestBlockNum
}

func (i *Indexer[T]) Close(ctx context.Context) error {
	return i.Flush(ctx)
}
