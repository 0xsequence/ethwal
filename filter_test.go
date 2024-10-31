package ethwal

import (
	"context"
	"fmt"
	"math"
	"os"
	"testing"

	"github.com/0xsequence/ethkit/go-ethereum/common"
	"github.com/0xsequence/ethwal/storage"
	"github.com/stretchr/testify/assert"
)

var (
	indexTestDir = ".tmp/ethwal_index_test"
)

func setupMockData[T any](indexGenerator func() Indexes[T], blockGenerator func() []Block[T]) (*Indexer[T], Indexes[T], storage.FS, func(), error) {
	indexes := indexGenerator()
	indexer, err := NewIndexer(context.Background(), IndexerOptions[T]{
		Dataset: Dataset{Path: indexTestDir},
		Indexes: indexes,
	})
	if err != nil {
		return nil, nil, nil, nil, err
	}
	blocks := blockGenerator()
	for _, block := range blocks {
		err := indexer.Index(context.Background(), block)
		if err != nil {
			return nil, nil, nil, nil, err
		}
	}
	err = indexer.Flush(context.Background())
	if err != nil {
		return nil, nil, nil, nil, err
	}

	return indexer, indexes, nil, cleanupIndexMockData(), nil
}

func cleanupIndexMockData() func() {
	return func() {
		err := os.RemoveAll(indexTestDir)
		if err != nil {
			panic(err)
		}
	}
}

func generateMixedIntBlocks() []Block[[]int] {
	blocks := []Block[[]int]{}

	// 0-19 generate 20 blocks with only even data
	// 20-39 generate 20 blocks with only odd data
	// 40-44 generate 5 blocks with even + odd data
	// 45-49 generate 5 blocks with no data
	// 50-69 generate 20 blocks with random but repeating huge numbers

	for i := 1; i <= 20; i++ {
		blocks = append(blocks, Block[[]int]{
			Hash:   common.BytesToHash([]byte{byte(i)}),
			Number: uint64(i),
			Data:   []int{i * 2},
		})
	}

	for i := 21; i <= 40; i++ {
		blocks = append(blocks, Block[[]int]{
			Hash:   common.BytesToHash([]byte{byte(i)}),
			Number: uint64(i),
			Data:   []int{i*2 + 1},
		})
	}

	for i := 41; i <= 45; i++ {
		blocks = append(blocks, Block[[]int]{
			Hash:   common.BytesToHash([]byte{byte(i)}),
			Number: uint64(i),
			Data:   []int{i*2 + 1, i*2 + 2},
		})
	}

	for i := 46; i <= 50; i++ {
		blocks = append(blocks, Block[[]int]{
			Hash:   common.BytesToHash([]byte{byte(i)}),
			Number: uint64(i),
			Data:   []int{},
		})
	}

	numbers := []int{
		121,
		123,
		125,
		999,
		777,
		333,
		555,
		111,
	}

	for i := 51; i < 71; i++ {
		data := []int{}
		for j := i; j < i+20; j++ {
			data = append(data, numbers[j%len(numbers)])
		}

		blocks = append(blocks, Block[[]int]{
			Hash:   common.BytesToHash([]byte{byte(i)}),
			Number: uint64(i),
			Data:   data,
		})
	}

	return blocks
}

func generateIntBlocks() []Block[[]int] {
	blocks := []Block[[]int]{}

	for i := 0; i < 100; i++ {
		blocks = append(blocks, Block[[]int]{
			Hash:   common.BytesToHash([]byte{byte(i)}),
			Number: uint64(i),
			Data:   []int{i, i + 1},
		})
	}

	return blocks
}

func generateMixedIntIndexes() Indexes[[]int] {
	indexes := Indexes[[]int]{}
	indexes["all"] = NewIndex[[]int]("all", indexBlock)
	indexes["odd_even"] = NewIndex[[]int]("odd_even", indexOddEvenBlocks)
	indexes["only_even"] = NewIndex[[]int]("only_even", indexOnlyEvenBlocks)
	indexes["only_odd"] = NewIndex[[]int]("only_odd", indexOnlyOddBlocks)
	return indexes
}

func generateIntIndexes() Indexes[[]int] {
	indexes := Indexes[[]int]{}
	indexes["all"] = NewIndex[[]int]("all", indexAll)
	indexes["none"] = NewIndex[[]int]("none", indexNone)
	return indexes
}

func indexOddEvenBlocks(block Block[[]int]) (toIndex bool, indexValueMap map[IndexedValue][]uint16, err error) {
	if len(block.Data) == 0 {
		return false, nil, nil
	}

	toIndex = true
	indexValueMap = make(map[IndexedValue][]uint16)
	indexValueMap["even"] = []uint16{}
	indexValueMap["odd"] = []uint16{}
	for i, data := range block.Data {
		if data%2 == 0 {
			indexValueMap["even"] = append(indexValueMap["even"], uint16(i))
		} else {
			indexValueMap["odd"] = append(indexValueMap["odd"], uint16(i))
		}
	}

	return
}

func indexOnlyEvenBlocks(block Block[[]int]) (toIndex bool, indexValueMap map[IndexedValue][]uint16, err error) {
	if len(block.Data) == 0 {
		return false, nil, nil
	}

	toIndex = true
	indexValueMap = make(map[IndexedValue][]uint16)
	for _, data := range block.Data {
		if data%2 != 0 {
			toIndex = false
			break
		}
	}

	if toIndex {
		indexValueMap["true"] = []uint16{math.MaxUint16}
	}

	return
}
func indexOnlyOddBlocks(block Block[[]int]) (toIndex bool, indexValueMap map[IndexedValue][]uint16, err error) {
	if len(block.Data) == 0 {
		return false, nil, nil
	}

	toIndex = true
	indexValueMap = make(map[IndexedValue][]uint16)
	for _, data := range block.Data {
		if data%2 == 0 {
			toIndex = false
			break
		}
	}

	if toIndex {
		indexValueMap["true"] = []uint16{math.MaxUint16}
	}

	return
}

func indexBlock(block Block[[]int]) (toIndex bool, indexValueMap map[IndexedValue][]uint16, err error) {
	if len(block.Data) == 0 {
		return false, nil, nil
	}

	if block.Number < 50 {
		return false, nil, nil
	}

	toIndex = true
	indexValueMap = make(map[IndexedValue][]uint16)
	for i, data := range block.Data {
		dataStr := IndexedValue(fmt.Sprintf("%d", data))
		if _, ok := indexValueMap[dataStr]; !ok {
			indexValueMap[dataStr] = []uint16{}
		}
		indexValueMap[dataStr] = append(indexValueMap[dataStr], uint16(i))
	}

	return
}

func indexAll(block Block[[]int]) (toIndex bool, indexValueMap map[IndexedValue][]uint16, err error) {
	if len(block.Data) == 0 {
		return false, nil, nil
	}

	toIndex = true
	indexValueMap = make(map[IndexedValue][]uint16)
	for _, data := range block.Data {
		dataStr := IndexedValue(fmt.Sprintf("%d", data))
		if _, ok := indexValueMap[dataStr]; !ok {
			indexValueMap[dataStr] = []uint16{math.MaxUint16}
		}
	}

	return
}

func indexNone(block Block[[]int]) (toIndex bool, indexValueMap map[IndexedValue][]uint16, err error) {
	return false, nil, nil
}

func TestMaxMagicCompoundID(t *testing.T) {
	id := NewIndexCompoundID(uint64(math.Exp2(48)-1), math.MaxUint16)
	assert.Equal(t, uint64(math.Exp2(48)-1), id.BlockNumber())
	assert.Equal(t, uint16(math.MaxUint16), id.DataIndex())
}

func TestIntMixFiltering(t *testing.T) {
	_, indexes, _, cleanup, err := setupMockData(generateMixedIntIndexes, generateMixedIntBlocks)
	assert.NoError(t, err)
	defer cleanup()

	f, err := NewFilterBuilder(context.Background(), FilterBuilderOptions[[]int]{
		Dataset: Dataset{
			Path: indexTestDir,
		},
		Indexes: indexes,
	})
	assert.NoError(t, err)
	assert.NotNil(t, f)

	onlyEvenFilter := f.Eq("only_even", "true")
	onlyOddFilter := f.Eq("only_odd", "true")
	oddFilter := f.Eq("odd_even", "odd")
	numbersIdxs := []string{
		"121",
		"123",
		"125",
		"999",
		"777",
		"333",
		"555",
		"111",
	}
	var numberFilter Filter
	for _, number := range numbersIdxs {
		if numberFilter == nil {
			numberFilter = f.Eq("all", number)
		} else {
			numberFilter = f.Or(numberFilter, f.Eq("all", number))
		}
	}

	onlyEvenResults := onlyEvenFilter.Eval()
	assert.Len(t, onlyEvenResults.Bitmap().ToArray(), 20)
	for _, id := range onlyEvenResults.Bitmap().ToArray() {
		block, _ := IndexCompoundID(id).Split()
		assert.True(t, block <= 20)
	}

	onlyOddResults := onlyOddFilter.Eval()
	assert.Len(t, onlyOddResults.Bitmap().ToArray(), 20+20)
	for _, id := range onlyOddResults.Bitmap().ToArray() {
		block, _ := IndexCompoundID(id).Split()
		assert.True(t, (block > 20 && block < 41) || (block > 50 && block < 71))
	}

	numberAllResults := numberFilter.Eval()
	// 20*20
	assert.Len(t, numberAllResults.Bitmap().ToArray(), 400)
	for _, id := range numberAllResults.Bitmap().ToArray() {
		block, _ := IndexCompoundID(id).Split()
		assert.True(t, block > 50 && block < 71)
	}

	allNumberAndOdd := f.And(numberFilter, oddFilter)
	allNumberOddResults := allNumberAndOdd.Eval()
	assert.ElementsMatch(t, numberAllResults.Bitmap().ToArray(), allNumberOddResults.Bitmap().ToArray())
}

func TestFiltering(t *testing.T) {
	_, indexes, _, cleanup, err := setupMockData(generateIntIndexes, generateIntBlocks)
	assert.NoError(t, err)
	defer cleanup()

	f, err := NewFilterBuilder(context.Background(), FilterBuilderOptions[[]int]{
		Dataset: Dataset{Path: indexTestDir},
		Indexes: indexes,
	})
	assert.NoError(t, err)
	assert.NotNil(t, f)
	result := f.Or(f.And(f.Eq("all", "1"), f.Eq("all", "2")), f.Eq("all", "3")).Eval()
	// result should contain block 1, 2, 3
	assert.Len(t, result.Bitmap().ToArray(), 3)
	block, _ := result.Next()
	assert.Equal(t, uint64(1), block)
	block, _ = result.Next()
	assert.Equal(t, uint64(2), block)
	block, _ = result.Next()
	assert.Equal(t, uint64(3), block)

	result = f.And(f.Eq("all", "1"), f.Eq("all", "2")).Eval()
	// result should contain block 1
	assert.Len(t, result.Bitmap().ToArray(), 1)
	block, _ = result.Next()
	assert.Equal(t, uint64(1), block)
}

func TestLowestIndexedBlockNum(t *testing.T) {
	indexer, indexes, _, cleanup, err := setupMockData(generateIntIndexes, generateIntBlocks)
	assert.NoError(t, err)
	defer cleanup()

	blockNum := indexer.BlockNum()
	assert.Equal(t, uint64(99), blockNum)

	for _, i := range indexes {
		i.numBlocksIndexed = nil
		block, err := i.LastBlockNumIndexed(context.Background(), indexer.fs)
		assert.NoError(t, err)
		assert.Equal(t, uint64(99), block)
	}

	indexes = generateIntIndexes()
	indexer, err = NewIndexer(context.Background(), IndexerOptions[[]int]{
		Dataset: Dataset{Path: indexTestDir},
		Indexes: indexes,
	})
	assert.NoError(t, err)
	lowestBlockIndexed := indexer.BlockNum()
	assert.Equal(t, uint64(99), lowestBlockIndexed)

	// add another filter...
	// indexes["odd_even"] = NewIndex("odd_even", indexOddEvenBlocks)
	// setup fresh objects
	indexes["odd_even"] = NewIndex("odd_even", indexOddEvenBlocks)
	indexer, err = NewIndexer(context.Background(), IndexerOptions[[]int]{
		Dataset: Dataset{Path: indexTestDir},
		Indexes: indexes,
	})
	assert.NoError(t, err)
	lowestBlockIndexed = indexer.BlockNum()
	assert.Equal(t, uint64(0), lowestBlockIndexed)
	blocks := generateIntBlocks()
	for _, block := range blocks[:50] {
		err = indexer.Index(context.Background(), block)
		assert.NoError(t, err)
	}
	err = indexer.Flush(context.Background())
	assert.NoError(t, err)
	lowestBlockIndexed = indexer.BlockNum()
	assert.Equal(t, uint64(49), lowestBlockIndexed)

	// index more blocks
	for _, block := range blocks[50:] {
		err = indexer.Index(context.Background(), block)
		assert.NoError(t, err)
	}
	err = indexer.Flush(context.Background())
	assert.NoError(t, err)
	lowestBlockIndexed = indexer.BlockNum()
	assert.Equal(t, uint64(99), lowestBlockIndexed)
}
