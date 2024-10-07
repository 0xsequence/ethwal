package ethwal

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMagicCompoundID(t *testing.T) {
	id := NewIndexCompoundID(101, math.MaxUint16)
	assert.Equal(t, uint64(101), id.BlockNumber())
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
	fiveFiveFive := f.Eq("all", "555")
	// numbersIdxs := []string{
	// 	"123",
	// 	"512",
	// 	"654",
	// 	"234",
	// 	"765",
	// 	"333",
	// 	"222",
	// 	"111",
	// }

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

	fiveFiveFiveResults := fiveFiveFive.Eval()
	assert.Len(t, fiveFiveFiveResults.Bitmap().ToArray(), 51)
	for _, id := range fiveFiveFiveResults.Bitmap().ToArray() {
		block, _ := IndexCompoundID(id).Split()
		assert.True(t, block > 49 && block < 70)
	}

	fiveFiveFiveOdd := f.And(fiveFiveFive, oddFilter)
	fiveFiveFiveOddResults := fiveFiveFiveOdd.Eval()
	assert.ElementsMatch(t, fiveFiveFiveResults.Bitmap().ToArray(), fiveFiveFiveOddResults.Bitmap().ToArray())
}
