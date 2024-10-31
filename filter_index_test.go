package ethwal

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMaxMagicCompoundID(t *testing.T) {
	id := NewIndexCompoundID(uint64(math.Exp2(48)-1), math.MaxUint16)
	assert.Equal(t, uint64(math.Exp2(48)-1), id.BlockNumber())
	assert.Equal(t, uint16(math.MaxUint16), id.DataIndex())
}

func TestIntMixFiltering(t *testing.T) {
	_, indexes, fs, cleanup, err := setupMockData("int_mix", generateMixedIntIndexes, generateMixedIntBlocks)
	assert.NoError(t, err)
	defer cleanup()

	f, err := NewFilterBuilder(indexes, fs)
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
	_, indexes, fs, cleanup, err := setupMockData("int_filtering", generateIntIndexes, generateIntBlocks)
	assert.NoError(t, err)
	defer cleanup()

	f, err := NewFilterBuilder(indexes, fs)
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
	builder, indexes, fs, cleanup, err := setupMockData("int_indexing_num", generateIntIndexes, generateIntBlocks)
	assert.NoError(t, err)
	defer cleanup()

	blockNum, err := builder.LastIndexedBlockNum(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, uint64(99), blockNum)

	for _, i := range indexes {
		i.numBlocksIndexed = nil
		block, err := i.LastBlockNumIndexed(context.Background(), fs)
		assert.NoError(t, err)
		assert.Equal(t, uint64(99), block)
	}

	indexes = generateIntIndexes()
	builder, err = NewIndexBuilder(context.Background(), indexes, fs)
	assert.NoError(t, err)
	lowestBlockIndexed, err := builder.LastIndexedBlockNum(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, uint64(99), lowestBlockIndexed)

	// add another filter...
	// indexes["odd_even"] = NewIndex("odd_even", indexOddEvenBlocks)
	// setup fresh objects
	indexes["odd_even"] = NewIndex("odd_even", indexOddEvenBlocks)
	builder, err = NewIndexBuilder(context.Background(), indexes, fs)
	assert.NoError(t, err)
	lowestBlockIndexed, err = builder.LastIndexedBlockNum(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), lowestBlockIndexed)
	blocks := generateIntBlocks()
	for _, block := range blocks[:50] {
		err = builder.Index(context.Background(), block)
		assert.NoError(t, err)
	}
	err = builder.Flush(context.Background())
	assert.NoError(t, err)
	lowestBlockIndexed, err = builder.LastIndexedBlockNum(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, uint64(49), lowestBlockIndexed)

	// index more blocks
	for _, block := range blocks[50:] {
		err = builder.Index(context.Background(), block)
		assert.NoError(t, err)
	}
	err = builder.Flush(context.Background())
	assert.NoError(t, err)
	lowestBlockIndexed, err = builder.LastIndexedBlockNum(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, uint64(99), lowestBlockIndexed)
}
