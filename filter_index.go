package ethwal

import (
	"context"
	"crypto/sha256"
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

func (i IndexName) Normalize() IndexName {
	return IndexName(strings.ToLower(string(i)))
}

type Indexes[T any] map[IndexName]IndexFunction[T]

type Index[T any] interface {
	Fetch(ctx context.Context, key string) (*roaring64.Bitmap, error)
	Store(ctx context.Context, block Block[T]) error
	Name() IndexName
}

type index[T any] struct {
	name      IndexName
	indexFunc IndexFunction[T]
	fs        storage.FS
}

var _ Index[any] = (*index[any])(nil)

func (i *index[T]) Fetch(ctx context.Context, indexValue string) (*roaring64.Bitmap, error) {
	file, err := newIndexFile(i.fs, i.name, indexValue)
	if err != nil {
		return nil, fmt.Errorf("failed to open index file: %w", err)
	}
	bmap, err := file.Read(ctx)
	if err != nil {
		return nil, err
	}

	return bmap, nil
}

func (i *index[T]) Store(ctx context.Context, block Block[T]) error {
	toIndex, indexValues, indexPositions, err := i.indexFunc(block)
	if err != nil {
		return fmt.Errorf("failed to index block: %w", err)
	}
	if !toIndex {
		return nil
	}

	indexValueMap := make(map[string][]IndexCompoundID)
	for i, indexValue := range indexValues {
		if _, ok := indexValueMap[indexValue]; !ok {
			indexValueMap[indexValue] = make([]IndexCompoundID, 0)
		}
		indexValueMap[indexValue] = append(indexValueMap[indexValue], NewIndexCompoundID(block.Number, indexPositions[i]))
	}

	for indexValue, indexIDs := range indexValueMap {
		file, err := newIndexFile(i.fs, i.name, indexValue)
		if err != nil {
			return fmt.Errorf("failed to open or create index file: %w", err)
		}
		bmap, err := file.Read(ctx)
		if err != nil {
			return err
		}
		for _, indexID := range indexIDs {
			bmap.Add(uint64(indexID))
		}
		err = file.Write(ctx, bmap)
		if err != nil {
			return err
		}
	}

	return nil
}

func (i *index[T]) Name() IndexName {
	return i.name
}

func indexPath(index string, indexValue string) string {
	h := sha256.New()
	h.Write([]byte(indexValue))
	hashed := h.Sum(nil)
	hashString := fmt.Sprintf("%x", hashed)
	dividedLen := int(len(hashString) / 4)
	indexValues := make([]string, 4)
	for i := 0; i < 4; i++ {
		if i == 3 {
			indexValues[i] = hashString[i*dividedLen:]
			continue
		}
		indexValues[i] = hashString[i*dividedLen : (i+1)*dividedLen]
	}
	return fmt.Sprintf("%s/%s/%s/%s/%s", index, indexValues[0], indexValues[1], indexValues[2], indexValues[3])
}
