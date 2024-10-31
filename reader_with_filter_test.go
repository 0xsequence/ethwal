package ethwal

import (
	"context"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupReaderWithFilterTest(t *testing.T) Indexes[[]int] {
	opt := Options{
		Dataset: Dataset{
			Path: testPath,
		},
		NewCompressor:   NewZSTDCompressor,
		NewDecompressor: NewZSTDDecompressor,
		NewEncoder:      NewCBOREncoder,
		NewDecoder:      NewCBORDecoder,
		FileRollOnClose: true,
	}

	w, err := NewWriter[[]int](opt)
	require.NoError(t, err)

	blocks := generateMixedIntBlocks()
	for _, block := range blocks {
		err := w.Write(context.Background(), block)
		require.NoError(t, err)
	}

	w.Close(context.Background())

	indexes := generateMixedIntIndexes()

	ib, err := NewIndexer(context.Background(), IndexerOptions[[]int]{
		Dataset: opt.Dataset,
		Indexes: indexes,
	})
	require.NoError(t, err)

	for _, block := range blocks {
		err := ib.Index(context.Background(), block)
		require.NoError(t, err)
	}

	err = ib.Flush(context.Background())
	require.NoError(t, err)

	return indexes
}

func teardownReaderWithFilterTest() {
	_ = os.RemoveAll(testPath)
}

func TestReaderWithFilter(t *testing.T) {
	indexes := setupReaderWithFilterTest(t)
	defer teardownReaderWithFilterTest()

	r, err := NewReader[[]int](Options{
		Dataset: Dataset{
			Path: testPath,
		},
		NewDecompressor: NewZSTDDecompressor,
		NewDecoder:      NewCBORDecoder,
	})
	require.NoError(t, err)

	fb, err := NewFilterBuilder(context.Background(), FilterBuilderOptions[[]int]{
		Dataset: Dataset{
			Path: testPath,
		},
		Indexes: indexes,
	})
	require.NoError(t, err)

	r, err = NewReaderWithFilter[[]int](r, fb.Eq("only_even", "true"))
	require.NoError(t, err)

	for {
		block, err := r.Read(context.Background())
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)

		for _, i := range block.Data {
			assert.Equal(t, 0, i%2)
		}
	}

	_ = r.Close()

	r, err = NewReader[[]int](Options{
		Dataset: Dataset{
			Path: testPath,
		},
		NewDecompressor: NewZSTDDecompressor,
		NewDecoder:      NewCBORDecoder,
	})
	require.NoError(t, err)

	r, err = NewReaderWithFilter[[]int](r, fb.Eq("only_odd", "true"))
	require.NoError(t, err)

	for {
		block, err := r.Read(context.Background())
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)

		for _, i := range block.Data {
			assert.Equal(t, 1, i%2)
		}
	}

	_ = r.Close()
}
