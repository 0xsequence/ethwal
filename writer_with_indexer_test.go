package ethwal

import (
	"context"
	"os"
	"path"
	"testing"

	"github.com/0xsequence/ethwal/storage/local"
	"github.com/stretchr/testify/require"
)

func TestWriterWithIndexer(t *testing.T) {
	defer func() {
		_ = os.RemoveAll(testPath)
	}()

	blocks := generateMixedIntBlocks()

	indexes := generateMixedIntIndexes(local.NewLocalFS(path.Join(testPath, ".indexes")))

	indexer, err := NewIndexer(context.Background(), indexes)
	require.NoError(t, err)

	w, err := NewWriter[[]int](Options{
		Dataset: Dataset{
			Path: testPath,
		},
		NewCompressor: NewZSTDCompressor,
		NewEncoder:    NewCBOREncoder,
	})
	require.NoError(t, err)

	wi, err := NewWriterWithIndexer(w, indexer)
	require.NoError(t, err)

	for _, block := range blocks {
		err := wi.Write(context.Background(), block)
		require.NoError(t, err)
	}

	err = wi.RollFile(context.Background())
	require.NoError(t, err)

	err = wi.Close(context.Background())
	require.NoError(t, err)

	indexDirEntries, err := os.ReadDir(path.Join(testPath, ".indexes"))
	require.NoError(t, err)
	require.Len(t, indexDirEntries, 4)

	ethwalDirEntries, err := os.ReadDir(testPath)
	require.NoError(t, err)
	require.Len(t, ethwalDirEntries, 3)
}
