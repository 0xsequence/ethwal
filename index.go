package ethwal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"sync/atomic"

	"github.com/0xsequence/ethwal/storage"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

// client could also do -> to index all positions in a block indexValueMap[indexValue\ -> {maxUint16}
// instead of appending all positions
type IndexFunction[T any] func(block Block[T]) (toIndex bool, indexValueMap map[string][]uint16, err error)

type IndexCompoundID uint64

func NewIndexCompoundID(blockNum uint64, dataIndex uint16) IndexCompoundID {
	return IndexCompoundID(uint64(blockNum<<16 | uint64(dataIndex)))
}

func (i IndexCompoundID) BlockNumber() uint64 {
	return (uint64(i) & 0xFFFFFFFFFFFF0000) >> 16
}

func (i IndexCompoundID) DataIndex() uint16 {
	return uint16(i) & 0xFFFF
}

func (i IndexCompoundID) Split() (uint64, uint16) {
	return i.BlockNumber(), i.DataIndex()
}

type IndexName string

type IndexValue string

func (i IndexName) Normalize() IndexName {
	return IndexName(strings.ToLower(string(i)))
}

type Indexes[T any] map[IndexName]Index[T]

type Index[T any] struct {
	name             IndexName
	indexFunc        IndexFunction[T]
	numBlocksIndexed *atomic.Uint64
}

func NewIndex[T any](name IndexName, indexFunc IndexFunction[T]) Index[T] {
	return Index[T]{
		name:      name.Normalize(),
		indexFunc: indexFunc,
	}
}

func (i *Index[T]) Fetch(ctx context.Context, fs storage.FS, indexValue IndexValue) (*roaring64.Bitmap, error) {
	file, err := NewIndexFile(fs, i.name, indexValue)
	if err != nil {
		return nil, fmt.Errorf("failed to open Index file: %w", err)
	}
	bmap, err := file.Read(ctx)
	if err != nil {
		return nil, err
	}

	return bmap, nil
}

func (i *Index[T]) Index(ctx context.Context, fs storage.FS, block Block[T]) (map[IndexValue]*roaring64.Bitmap, error) {
	numBlocksIndexed, err := i.NumBlocksIndexed(ctx, fs)
	if err != nil {
		return nil, fmt.Errorf("unexpected: failed to get number of blocks indexed: %w", err)
	}

	if block.Number <= numBlocksIndexed {
		return nil, nil
	}

	toIndex, indexValueMap, err := i.indexFunc(block)
	if err != nil {
		return nil, fmt.Errorf("failed to Index block: %w", err)
	}
	if !toIndex {
		return map[IndexValue]*roaring64.Bitmap{}, nil
	}

	indexValueCompoundMap := make(map[string][]IndexCompoundID)
	for indexValue, positions := range indexValueMap {
		if _, ok := indexValueMap[indexValue]; !ok {
			indexValueCompoundMap[indexValue] = make([]IndexCompoundID, 0)
		}
		for _, pos := range positions {
			indexValueCompoundMap[indexValue] = append(indexValueCompoundMap[indexValue], NewIndexCompoundID(block.Number, pos))
		}
	}

	indexValueBitmapMap := make(map[IndexValue]*roaring64.Bitmap)
	for indexValue, indexIDs := range indexValueCompoundMap {
		bm, ok := indexValueBitmapMap[IndexValue(indexValue)]
		if !ok {
			bm = roaring64.New()
			indexValueBitmapMap[IndexValue(indexValue)] = bm
		}

		for _, indexID := range indexIDs {
			bm.Add(uint64(indexID))
		}
	}
	return indexValueBitmapMap, nil
}

func (i *Index[T]) Store(ctx context.Context, fs storage.FS, indexValuesBitmapsMap map[IndexValue]*roaring64.Bitmap, maxBlock uint64) error {
	numBlocksIndexed, err := i.NumBlocksIndexed(ctx, fs)
	if err != nil {
		return fmt.Errorf("failed to get number of blocks indexed: %w", err)
	}
	if maxBlock <= numBlocksIndexed {
		return nil
	}

	for indexValue, bmUpdate := range indexValuesBitmapsMap {
		if bmUpdate.IsEmpty() {
			continue
		}

		file, err := NewIndexFile(fs, i.name, indexValue)
		if err != nil {
			return fmt.Errorf("failed to open or create Index file: %w", err)
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

	err = i.storeNumBlocksIndexed(ctx, fs, maxBlock)
	if err != nil {
		return fmt.Errorf("failed to store number of blocks indexed: %w", err)
	}

	return nil
}

func (i *Index[T]) Name() IndexName {
	return i.name
}

func (i *Index[T]) NumBlocksIndexed(ctx context.Context, fs storage.FS) (uint64, error) {
	if i.numBlocksIndexed != nil {
		return i.numBlocksIndexed.Load(), nil
	}

	file, err := fs.Open(ctx, indexBlocksIndexedPath(string(i.name)), nil)
	if err != nil {
		// file doesn't exist
		return 0, nil
	}
	defer file.Close()

	buf, err := io.ReadAll(file)
	if err != nil {
		return 0, fmt.Errorf("failed to read Index file: %w", err)
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

func (i *Index[T]) storeNumBlocksIndexed(ctx context.Context, fs storage.FS, numBlocksIndexed uint64) error {
	var prevBlockIndexed uint64
	blocksIndexed, err := i.NumBlocksIndexed(ctx, fs)
	if err == nil {
		prevBlockIndexed = blocksIndexed
	}

	if prevBlockIndexed >= numBlocksIndexed {
		return nil
	}

	file, err := fs.Create(ctx, indexBlocksIndexedPath(string(i.name)), nil)
	if err != nil {
		return fmt.Errorf("failed to open Index file: %w", err)
	}
	defer file.Close()

	err = binary.Write(file, binary.BigEndian, numBlocksIndexed)
	if err != nil {
		return fmt.Errorf("failed to write Index file: %w", err)
	}

	if i.numBlocksIndexed == nil {
		i.numBlocksIndexed = &atomic.Uint64{}
	}
	i.numBlocksIndexed.Store(numBlocksIndexed)
	return nil
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

func indexBlocksIndexedPath(index string) string {
	return fmt.Sprintf("%s/%s", index, "indexed")
}
