package ethwal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strings"
	"sync/atomic"

	"github.com/0xsequence/ethwal/storage"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

// IndexAllDataIndexes is a special position that indicates that all data indexes should be indexed.
const IndexAllDataIndexes = math.MaxUint16

// IndexFunction is a function that indexes a block.
//
// The function should return true if the block should be indexed, and false otherwise.
// The function should return an error if the indexing fails.
// The function should return a map of index values to positions in the block.
type IndexFunction[T any] func(block Block[T]) (toIndex bool, indexValueMap map[IndexedValue][]uint16, err error)

// IndexName is the name of an index.
type IndexName string

func (i IndexName) Normalize() IndexName {
	return IndexName(strings.ToLower(string(i)))
}

// IndexedValue is the indexed value of an index.
type IndexedValue string

// IndexUpdate is a map of indexed values and their corresponding bitmaps.
type IndexUpdate struct {
	BlockBitmap     map[IndexedValue]*roaring64.Bitmap
	DataIndexBitmap map[IndexedValue]*roaring64.Bitmap
	LastBlockNum    uint64
}

func (u *IndexUpdate) Merge(update *IndexUpdate) {
	for indexValue, bm := range update.BlockBitmap {
		if _, ok := u.BlockBitmap[indexValue]; !ok {
			u.BlockBitmap[indexValue] = roaring64.New()
		}
		u.BlockBitmap[indexValue].Or(bm)
	}

	if u.LastBlockNum < update.LastBlockNum {
		u.LastBlockNum = update.LastBlockNum
	}
}

// Indexes is a map of index names to indexes.
type Indexes[T any] map[IndexName]Index[T]

// Index is an index struct.
type Index[T any] struct {
	name      IndexName
	indexFunc IndexFunction[T]

	numBlocksIndexed *atomic.Uint64
}

func NewIndex[T any](name IndexName, indexFunc IndexFunction[T]) Index[T] {
	return Index[T]{
		name:      name.Normalize(),
		indexFunc: indexFunc,
	}
}

func (i *Index[T]) Name() IndexName {
	return i.name
}

func (i *Index[T]) Fetch(ctx context.Context, fs storage.FS, indexValue IndexedValue) (*roaring64.Bitmap, error) {
	file, err := NewIndexFile(fs, i.name, indexValue)
	if err != nil {
		return nil, fmt.Errorf("failed to open IndexBlock file: %w", err)
	}
	bmap, err := file.Read(ctx)
	if err != nil {
		return nil, err
	}

	return bmap, nil
}

func (i *Index[T]) IndexBlock(ctx context.Context, fs storage.FS, block Block[T]) (*IndexUpdate, error) {
	if fs != nil {
		numBlocksIndexed, err := i.LastBlockNumIndexed(ctx, fs)
		if err != nil {
			return nil, fmt.Errorf("unexpected: failed to get number of blocks indexed: %w", err)
		}

		if block.Number <= numBlocksIndexed {
			return nil, nil
		}
	}

	toIndex, indexValueMap, err := i.indexFunc(block)
	if err != nil {
		return nil, fmt.Errorf("failed to IndexBlock block: %w", err)
	}
	if !toIndex {
		return &IndexUpdate{LastBlockNum: block.Number}, nil
	}

	indexValueCompoundMap := make(map[IndexedValue]uint64)
	for indexValue, _ := range indexValueMap {
		if _, ok := indexValueCompoundMap[indexValue]; !ok {
			indexValueCompoundMap[indexValue] = block.Number
		}
	}

	indexUpdate := &IndexUpdate{
		BlockBitmap:     make(map[IndexedValue]*roaring64.Bitmap),
		DataIndexBitmap: make(map[IndexedValue]*roaring64.Bitmap),
		LastBlockNum:    block.Number,
	}
	for indexValue, blockNumber := range indexValueCompoundMap {
		bm, ok := indexUpdate.BlockBitmap[indexValue]
		if !ok {
			bm = roaring64.New()
			indexUpdate.BlockBitmap[indexValue] = bm
		}
		bm.Add(blockNumber)

		dataIndexBM, ok := indexUpdate.DataIndexBitmap[indexValue]
		if !ok {
			dataIndexBM = roaring64.New()
			indexUpdate.DataIndexBitmap[indexValue] = dataIndexBM
		}
		for _, dataIndex := range indexValueMap[indexValue] {
			dataIndexBM.Add(uint64(dataIndex))
		}
	}

	return indexUpdate, nil
}

func (i *Index[T]) Store(ctx context.Context, fs storage.FS, indexUpdate *IndexUpdate) error {
	lastBlockNumIndexed, err := i.LastBlockNumIndexed(ctx, fs)
	if err != nil {
		return fmt.Errorf("failed to get number of blocks indexed: %w", err)
	}
	if lastBlockNumIndexed >= indexUpdate.LastBlockNum {
		return nil
	}

	for indexValue, bmUpdate := range indexUpdate.BlockBitmap {
		if bmUpdate.IsEmpty() {
			continue
		}

		file, err := NewIndexFile(fs, i.name, indexValue)
		if err != nil {
			return fmt.Errorf("failed to open or create IndexBlock file: %w", err)
		}

		bmap, err := file.Read(ctx)
		if err != nil {
			return err
		}

		bmap.Or(bmUpdate)

		err = file.Write(ctx, bmap)
		if err != nil {
			return err
		}
	}

	err = i.storeLastBlockNumIndexed(ctx, fs, indexUpdate.LastBlockNum)
	if err != nil {
		return fmt.Errorf("failed to index number of blocks indexed: %w", err)
	}

	return nil
}

func (i *Index[T]) LastBlockNumIndexed(ctx context.Context, fs storage.FS) (uint64, error) {
	if i.numBlocksIndexed != nil {
		return i.numBlocksIndexed.Load(), nil
	}

	file, err := fs.Open(ctx, indexedBlockNumFilePath(string(i.name)), nil)
	if err != nil {
		// file doesn't exist
		return 0, nil
	}
	defer file.Close()

	buf, err := io.ReadAll(file)
	if err != nil {
		return 0, fmt.Errorf("failed to read IndexBlock file: %w", err)
	}

	var numBlocksIndexed uint64
	err = binary.Read(bytes.NewReader(buf), binary.BigEndian, &numBlocksIndexed)
	if err != nil {
		return 0, fmt.Errorf("failed to unmarshal bitmap: %w", err)
	}

	i.numBlocksIndexed = &atomic.Uint64{}
	i.numBlocksIndexed.Store(numBlocksIndexed)

	return numBlocksIndexed, nil
}

func (i *Index[T]) storeLastBlockNumIndexed(ctx context.Context, fs storage.FS, numBlocksIndexed uint64) error {
	var prevBlockIndexed uint64
	blocksIndexed, err := i.LastBlockNumIndexed(ctx, fs)
	if err == nil {
		prevBlockIndexed = blocksIndexed
	}

	if prevBlockIndexed >= numBlocksIndexed {
		return nil
	}

	file, err := fs.Create(ctx, indexedBlockNumFilePath(string(i.name)), nil)
	if err != nil {
		return fmt.Errorf("failed to open IndexBlock file: %w", err)
	}

	err = binary.Write(file, binary.BigEndian, numBlocksIndexed)
	if err != nil {
		return fmt.Errorf("failed to write IndexBlock file: %w", err)
	}

	err = file.Close()
	if err != nil {
		return fmt.Errorf("failed to close IndexBlock file: %w", err)
	}

	if i.numBlocksIndexed == nil {
		i.numBlocksIndexed = &atomic.Uint64{}
	}
	i.numBlocksIndexed.Store(numBlocksIndexed)
	return nil
}

func indexedBlockNumFilePath(index string) string {
	return fmt.Sprintf("%s/%s", index, "indexed")
}

func indexPath(index string, indexValue string) string {
	hash := sha256.Sum224([]byte(indexValue))
	return fmt.Sprintf("%s/%06d/%06d/%06d/%s",
		index,
		binary.BigEndian.Uint64(hash[0:8])%NumberOfDirectoriesPerLevel,   // level0
		binary.BigEndian.Uint64(hash[8:16])%NumberOfDirectoriesPerLevel,  // level1
		binary.BigEndian.Uint64(hash[16:24])%NumberOfDirectoriesPerLevel, // level2
		fmt.Sprintf("%s.idx", indexValue),                                // filename
	)
}

type IndexIterator struct {
	bm   *roaring64.Bitmap
	iter roaring64.IntIterable64
}

func NewIndexIterator(bitmap *roaring64.Bitmap) *IndexIterator {
	return &IndexIterator{bm: bitmap, iter: bitmap.Iterator()}
}

func (i *IndexIterator) First() bool {
	i.iter = i.bm.Iterator()
	if i.iter.HasNext() {
		return true
	}
	return false
}

func (i *IndexIterator) Last() bool {
	i.iter = i.bm.ReverseIterator()
	if i.iter.HasNext() {
		return true
	}
	return false
}

func (i *IndexIterator) HasNext() bool {
	return i.iter.HasNext()
}

func (i *IndexIterator) Next() uint64 {
	return i.iter.Next()
}

func (i *IndexIterator) Peek() uint64 {
	if peekable, ok := i.iter.(roaring64.IntPeekable64); ok {
		return peekable.PeekNext()
	}
	return math.MaxUint64
}

func (i *IndexIterator) Bitmap() *roaring64.Bitmap {
	return i.bm
}
