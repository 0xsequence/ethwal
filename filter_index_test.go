package ethwal

import (
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

	f, err := NewIndexesFilterBuilder(indexes, fs)
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
		assert.True(t, block < 20)
	}

	onlyOddResults := onlyOddFilter.Eval()
	assert.Len(t, onlyOddResults.Bitmap().ToArray(), 20+20)
	for _, id := range onlyOddResults.Bitmap().ToArray() {
		block, _ := IndexCompoundID(id).Split()
		assert.True(t, (block > 19 && block < 40) || (block > 49 && block < 70))
	}

	numberAllResults := numberFilter.Eval()
	// 20*20
	assert.Len(t, numberAllResults.Bitmap().ToArray(), 400)
	for _, id := range numberAllResults.Bitmap().ToArray() {
		block, _ := IndexCompoundID(id).Split()
		assert.True(t, block > 49 && block < 70)
	}

	allNumberAndOdd := f.And(numberFilter, oddFilter)
	allNumberOddResults := allNumberAndOdd.Eval()
	assert.ElementsMatch(t, numberAllResults.Bitmap().ToArray(), allNumberOddResults.Bitmap().ToArray())
}
