package ethwal

import (
	"context"
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/0xsequence/ethwal/storage/local"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestFile(t *testing.T) *File {
	err := os.MkdirAll(testRoot, 0755)
	require.NoError(t, err)

	// setup
	file := &File{
		FirstBlockNum: 1,
		LastBlockNum:  49,
	}

	err = os.MkdirAll(path.Join(testRoot, path.Dir(file.Path())), 0755)
	require.NoError(t, err)

	fileOS, err := os.Create(path.Join(testRoot, file.Path()))
	require.NoError(t, err)

	_, err = fileOS.Write([]byte("hello world"))
	require.NoError(t, err)

	err = fileOS.Close()
	require.NoError(t, err)

	return file
}

func teardownTestFile(t *testing.T) {
	os.RemoveAll(testRoot)
}

// TestFile_Path does the sanity check for the File.Path() method.
func TestFile_Path(t *testing.T) {
	var files []string
	for i := 0; i < 1000; i++ {
		files = append(files, (&File{
			FirstBlockNum: uint64(i * 50),
			LastBlockNum:  uint64(i*50 + 49),
		}).Path())
	}

	for i, file := range files {
		for j, file2 := range files {
			if i != j {
				assert.NotEqual(t, file, file2)
			}
		}
	}
}

func TestFile_Open(t *testing.T) {
	t.Run("WhenExist", func(t *testing.T) {
		// setup
		file := setupTestFile(t)
		defer teardownTestFile(t)

		// test
		fs := local.NewLocalFS(testRoot)

		r, err := file.Open(context.Background(), fs)
		require.NoError(t, err)
		require.NotNil(t, r)
		defer r.Close()
	})

	t.Run("WhenNotExist", func(t *testing.T) {
		// setup
		file := &File{
			FirstBlockNum: 1,
			LastBlockNum:  49,
		}

		// test
		fs := local.NewLocalFS(testRoot)

		_, err := file.Open(context.Background(), fs)
		require.ErrorIs(t, err, ErrFileNotExist)
	})
}

func TestFile_Exist(t *testing.T) {
	t.Run("True", func(t *testing.T) {
		// setup
		// setup
		file := setupTestFile(t)
		defer teardownTestFile(t)

		// test
		fs := local.NewLocalFS(testRoot)

		exist := file.Exist(context.Background(), fs)
		assert.True(t, exist)
	})

	t.Run("False", func(t *testing.T) {
		// setup
		file := &File{
			FirstBlockNum: 1,
			LastBlockNum:  49,
		}

		// test
		fs := local.NewLocalFS(testRoot)

		exist := file.Exist(context.Background(), fs)
		assert.False(t, exist)
	})
}

func TestFile_Prefetch(t *testing.T) {
	t.Run("WhenExist", func(t *testing.T) {
		// setup
		file := setupTestFile(t)
		defer teardownTestFile(t)

		// test
		fs := local.NewLocalFS(testRoot)

		err := file.Prefetch(context.Background(), fs)
		require.NoError(t, err)

		assert.NotNil(t, file.prefetchBuffer)

		file.PrefetchClear()
		assert.Nil(t, file.prefetchBuffer)
	})

	t.Run("WhenExistClearByOpen", func(t *testing.T) {
		// setup
		file := setupTestFile(t)
		defer teardownTestFile(t)

		// test
		fs := local.NewLocalFS(testRoot)

		err := file.Prefetch(context.Background(), fs)
		require.NoError(t, err)

		assert.NotNil(t, file.prefetchBuffer)

		// get prefetch buffer
		err = file.Prefetch(context.Background(), fs)
		require.NoError(t, err)
		assert.NotNil(t, file.prefetchBuffer)

		f, err := file.Open(context.Background(), fs)
		defer f.Close()
		require.NoError(t, err)
		assert.NotNil(t, f)

		assert.Nil(t, file.prefetchBuffer)
	})

	t.Run("WhenNotExist", func(t *testing.T) {
		// setup
		file := &File{
			FirstBlockNum: 1,
			LastBlockNum:  49,
		}

		// test
		fs := local.NewLocalFS(testRoot)

		err := file.Prefetch(context.Background(), fs)
		require.ErrorIs(t, err, ErrFileNotExist)
		assert.Nil(t, file.prefetchBuffer)
	})
}

func TestNewFileIndex(t *testing.T) {
	testSetup(t, NewCBOREncoder, nil)
	defer testTeardown(t)

	fs := local.NewLocalFS(path.Join(testPath, "int-wal", defaultDatasetVersion))

	fileIndex := NewFileIndex(fs)

	err := fileIndex.Load(context.Background())
	require.NoError(t, err)
	require.NotNil(t, fileIndex)

	assert.Equal(t, fs, fileIndex.fs)
	assert.Len(t, fileIndex.files, 3)
}

func TestNewFileIndexFromFiles(t *testing.T) {
	var files []*File
	for i := 999; i >= 0; i-- {
		files = append(files, &File{
			FirstBlockNum: uint64(i * 50),
			LastBlockNum:  uint64(i*50 + 49),
		})
	}

	fi := NewFileIndexFromFiles(nil, files)
	assert.Equal(t, files, fi.files)
	assert.Equal(t, files, fi.Files())

	assert.Equal(t, fi.Files()[0].FirstBlockNum, uint64(0))
	assert.Equal(t, fi.Files()[0].LastBlockNum, uint64(49))
	assert.Equal(t, fi.Files()[999].FirstBlockNum, uint64(49950))
	assert.Equal(t, fi.Files()[999].LastBlockNum, uint64(49999))
}

func TestFileIndex_AddFile(t *testing.T) {
	var files []*File
	for i := 999; i >= 0; i-- {
		files = append(files, &File{
			FirstBlockNum: uint64(i*50 + 1),
			LastBlockNum:  uint64(i*50 + 50),
		})
	}

	fi := NewFileIndexFromFiles(nil, files)

	file := &File{
		FirstBlockNum: 50001,
		LastBlockNum:  50050,
	}
	err := fi.AddFile(file)
	require.NoError(t, err)
	assert.Equal(t, file, fi.At(1000))

	file = &File{
		FirstBlockNum: 50001,
		LastBlockNum:  50070,
	}
	err = fi.AddFile(file)
	require.Error(t, err)

	file = &File{
		FirstBlockNum: 0,
		LastBlockNum:  50070,
	}
	err = fi.AddFile(file)
	require.Error(t, err)
}

func TestFileIndex_At(t *testing.T) {
	var files []*File
	for i := 999; i >= 0; i-- {
		files = append(files, &File{
			FirstBlockNum: uint64(i * 50),
			LastBlockNum:  uint64(i*50 + 49),
		})
	}

	fi := NewFileIndexFromFiles(nil, files)
	assert.Equal(t, files[0], fi.At(0))
	assert.Equal(t, files[999], fi.At(999))
	assert.Nil(t, fi.At(1000))
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
	require.ErrorIs(t, err, ErrFileNotExist)
}

func TestFileIndex_Save(t *testing.T) {
	file := setupTestFile(t)
	defer teardownTestFile(t)

	fs := local.NewLocalFS(testRoot)
	fi := NewFileIndexFromFiles(fs, []*File{file})
	err := fi.Save(context.Background())
	require.NoError(t, err)

	_, err = os.Stat(path.Join(testRoot, FileIndexFileName))
	require.NoError(t, err)
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
