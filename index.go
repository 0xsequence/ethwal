package ethwal

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/0xsequence/ethwal/storage"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

type IndexFunction[T any] func(block Block[T]) (toIndex bool, indexValues []string, positions []uint16, err error)

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
	name      IndexName
	indexFunc IndexFunction[T]
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

func (i *Index[T]) Index(block Block[T]) (map[IndexValue]*roaring64.Bitmap, error) {
	toIndex, indexValues, indexPositions, err := i.indexFunc(block)
	if err != nil {
		return nil, fmt.Errorf("failed to Index block: %w", err)
	}
	if !toIndex {
		return map[IndexValue]*roaring64.Bitmap{}, nil
	}

	indexValueMap := make(map[string][]IndexCompoundID)
	for i, indexValue := range indexValues {
		if _, ok := indexValueMap[indexValue]; !ok {
			indexValueMap[indexValue] = make([]IndexCompoundID, 0)
		}
		indexValueMap[indexValue] = append(indexValueMap[indexValue], NewIndexCompoundID(block.Number, indexPositions[i]))
	}

	indexValueBitmapMap := make(map[IndexValue]*roaring64.Bitmap)
	for indexValue, indexIDs := range indexValueMap {
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

func (i *Index[T]) Store(ctx context.Context, fs storage.FS, indexValuesBitmapsMap map[IndexValue]*roaring64.Bitmap) error {
	for indexValue, bmUpdate := range indexValuesBitmapsMap {
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

	return nil
}

func (i *Index[T]) Name() IndexName {
	return i.name
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
