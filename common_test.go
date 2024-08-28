package ethwal

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFile_Path(t *testing.T) {
	t.Skip()
}

func TestFile_Open(t *testing.T) {
	t.Skip()
}

func TestFile_Exist(t *testing.T) {
	t.Skip()
}

func TestFile_Prefetch(t *testing.T) {
	t.Skip()
}

func TestNewFileIndex(t *testing.T) {
	t.Skip()
}

func TestNewFileIndexFromFiles(t *testing.T) {
	t.Skip()
}

func TestFileIndex_AddFile(t *testing.T) {
	t.Skip()
}

func TestFileIndex_At(t *testing.T) {
	t.Skip()
}

func TestFileIndex_FindFile(t *testing.T) {
	fileIndex := NewFileIndexFromFiles(nil, []*File{
		{FirstBlockNum: 0, LastBlockNum: 49},
		{FirstBlockNum: 50, LastBlockNum: 99},
		{FirstBlockNum: 100, LastBlockNum: 149},
		{FirstBlockNum: 150, LastBlockNum: 199},
	})

	for i := 0; i <= 49; i++ {
		_, index, err := fileIndex.FindFile(uint64(i))
		require.NoError(t, err)
		assert.Equal(t, 0, index)
	}

	for i := 50; i <= 99; i++ {
		_, index, err := fileIndex.FindFile(uint64(i))
		require.NoError(t, err)
		assert.Equal(t, 1, index)
	}

	for i := 100; i <= 149; i++ {
		_, index, err := fileIndex.FindFile(uint64(i))
		require.NoError(t, err)
		assert.Equal(t, 2, index)
	}

	for i := 150; i <= 199; i++ {
		_, index, err := fileIndex.FindFile(uint64(i))
		require.NoError(t, err)
		assert.Equal(t, 3, index)
	}

	_, _, err := fileIndex.FindFile(200)
	require.ErrorIs(t, err, ErrFileNotFound)
}

func TestFileIndex_Save(t *testing.T) {
	t.Skip()
}

func BenchmarkFindInFileIndex(b *testing.B) {
	benchCase := []struct {
		NumFiles uint64
	}{
		{NumFiles: 2000},
		{NumFiles: 20000},
		{NumFiles: 200000},
		{NumFiles: 2000000},
		{NumFiles: 20000000},
		{NumFiles: 200000000},
	}

	for _, bench := range benchCase {
		b.Run(fmt.Sprintf("NumFiles-%d", bench.NumFiles), func(b *testing.B) {
			files := make([]*File, 0, bench.NumFiles)
			for i := uint64(1); i < bench.NumFiles+1; i++ {
				files = append(files, &File{
					FirstBlockNum: (i - 1) * 50,
					LastBlockNum:  i,
				})
			}

			fi := NewFileIndexFromFiles(nil, files)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _, _ = fi.FindFile(bench.NumFiles / 3)
			}
			b.StopTimer()
		})
	}
}
