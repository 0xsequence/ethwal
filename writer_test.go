package ethwal

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"sync"
	"testing"

	"github.com/0xsequence/ethkit/go-ethereum/common"
	"github.com/0xsequence/ethwal/storage"
	"github.com/0xsequence/ethwal/storage/local"
	gostorage "github.com/Shopify/go-storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriter_Write(t *testing.T) {
	blocksFile := Blocks[int]{
		{
			Hash:   common.BytesToHash([]byte{0x01}),
			Number: 1,
			TS:     0,
			Data:   0,
		},
		{
			Hash:   common.BytesToHash([]byte{0x02}),
			Number: 2,
			TS:     0,
			Data:   0,
		},
		{
			Hash:   common.BytesToHash([]byte{0x03}),
			Number: 3,
			TS:     0,
			Data:   0,
		},
		{
			Hash:   common.BytesToHash([]byte{0x04}),
			Number: 4,
			TS:     0,
			Data:   0,
		},
	}

	testCase := []struct {
		name    string
		options Options
	}{
		{
			name: "json",
			options: Options{
				Dataset: Dataset{
					Name:    "int-wal",
					Path:    testPath,
					Version: defaultDatasetVersion,
				},
				NewEncoder: NewJSONEncoder,
				NewDecoder: NewJSONDecoder,
			},
		},
		{
			name: "json-zstd",
			options: Options{
				Dataset: Dataset{
					Name:    "int-wal",
					Path:    testPath,
					Version: defaultDatasetVersion,
				},
				NewEncoder:      NewJSONEncoder,
				NewDecoder:      NewJSONDecoder,
				NewCompressor:   NewZSTDCompressor,
				NewDecompressor: NewZSTDDecompressor,
			},
		},
		{
			name: "cbor",
			options: Options{
				Dataset: Dataset{
					Name:    "int-wal",
					Path:    testPath,
					Version: defaultDatasetVersion,
				},
				NewEncoder: NewCBOREncoder,
				NewDecoder: NewCBORDecoder,
			},
		},
		{
			name: "cbor-zstd",
			options: Options{
				Dataset: Dataset{
					Name:    "int-wal",
					Path:    testPath,
					Version: defaultDatasetVersion,
				},
				NewEncoder:      NewCBOREncoder,
				NewDecoder:      NewCBORDecoder,
				NewCompressor:   NewZSTDCompressor,
				NewDecompressor: NewZSTDDecompressor,
			},
		},
	}

	for _, tc := range testCase {
		t.Run(tc.name, func(t *testing.T) {
			defer testTeardown(t)

			tc.options = tc.options.WithDefaults()

			w, err := NewWriter[int](tc.options)
			require.NoError(t, err)

			for _, block := range blocksFile {
				err := w.Write(context.Background(), block)
				require.NoError(t, err)
			}

			// flush the in-memory buffer to disk
			w_, ok := w.(*writer[int])
			require.True(t, ok)

			err = w_.rollFile(context.Background())
			require.NoError(t, err)

			err = w.Close(context.Background())
			require.NoError(t, err)

			// check WAL files
			filePath := path.Join(buildETHWALPath(tc.options.Dataset.Name, tc.options.Dataset.Version, tc.options.Dataset.Path), (&File{FirstBlockNum: 1, LastBlockNum: 4}).Path())
			_, err = os.Stat(filePath)
			require.NoError(t, err)

			f, err := os.Open(filePath)
			require.NoError(t, err)

			var r io.ReadCloser = f
			if tc.options.NewDecompressor != nil {
				r = tc.options.NewDecompressor(r)
			}

			var dec = tc.options.NewDecoder(r)

			var blocks Blocks[int]
			for {
				var block Block[int]
				err := dec.Decode(&block)
				if err != nil {
					break
				}
				blocks = append(blocks, block)
			}

			require.Equal(t, blocksFile, blocks)
		})
	}
}

func TestWriter_Continue(t *testing.T) {
	defer testTeardown(t)

	// 1st writer
	w, err := NewWriter[int](Options{
		Dataset: Dataset{
			Name:    "int-wal",
			Path:    testPath,
			Version: defaultDatasetVersion,
		},
	})
	require.NoError(t, err)

	err = w.Write(context.Background(), Block[int]{Number: 1})
	require.NoError(t, err)

	// flush the in-memory buffer to disk
	w_, ok := w.(*writer[int])
	require.True(t, ok)

	err = w_.rollFile(context.Background())
	require.NoError(t, err)

	err = w.Close(context.Background())
	require.NoError(t, err)

	// 2nd writer
	w, err = NewWriter[int](Options{
		Dataset: Dataset{
			Name:    "int-wal",
			Path:    testPath,
			Version: defaultDatasetVersion,
		},
	})
	require.NoError(t, err)

	assert.Equal(t, uint64(1), w.BlockNum())

	err = w.Write(context.Background(), Block[int]{Number: 2})
	require.NoError(t, err)

	assert.Equal(t, uint64(2), w.BlockNum())

	err = w.Close(context.Background())
	require.NoError(t, err)
}

func TestNoGapWriter_BlockNum(t *testing.T) {
	defer testTeardown(t)

	w, err := NewWriter[int](Options{
		Dataset: Dataset{
			Name:    "int-wal",
			Path:    testPath,
			Version: defaultDatasetVersion,
		},
		NewEncoder: NewJSONEncoder,
	})
	require.NoError(t, err)

	ngw := NewWriterNoGap[int](w)
	require.NotNil(t, w)

	err = ngw.Write(context.Background(), Block[int]{Number: 1})
	require.NoError(t, err)

	err = ngw.Write(context.Background(), Block[int]{Number: 2})
	require.NoError(t, err)

	err = ngw.Write(context.Background(), Block[int]{Number: 3})
	require.NoError(t, err)

	err = ngw.Close(context.Background())
	require.NoError(t, err)

	require.Equal(t, uint64(3), w.BlockNum())
}

func TestNoGapWriter_FileRollOnClose(t *testing.T) {
	defer testTeardown(t)

	opt := Options{
		Dataset: Dataset{
			Name:    "int-wal",
			Path:    testPath,
			Version: defaultDatasetVersion,
		},
		NewEncoder:      NewJSONEncoder,
		FileRollOnClose: true,
	}

	w, err := NewWriter[int](opt)
	require.NoError(t, err)

	ngw := NewWriterNoGap[int](w)
	require.NotNil(t, w)

	err = ngw.Write(context.Background(), Block[int]{Number: 1})
	require.NoError(t, err)

	err = ngw.Write(context.Background(), Block[int]{Number: 2})
	require.NoError(t, err)

	err = ngw.Write(context.Background(), Block[int]{Number: 3})
	require.NoError(t, err)

	err = ngw.Close(context.Background())
	require.NoError(t, err)

	require.Equal(t, uint64(3), w.BlockNum())

	// check WAL files
	filePath := path.Join(buildETHWALPath(opt.Dataset.Name, opt.Dataset.Version, opt.Dataset.Path), (&File{FirstBlockNum: 1, LastBlockNum: 3}).Path())
	_, err = os.Stat(filePath)
	require.NoError(t, err)
}

func Test_WriterStoragePathSuffix(t *testing.T) {
	defer testTeardown(t)

	options := Options{
		Dataset: Dataset{
			Name:    "int-wal",
			Path:    testPath,
			Version: defaultDatasetVersion,
		},
	}

	w, err := NewWriter[int](options)
	require.NoError(t, err)

	writer, ok := w.(*writer[int])
	require.True(t, ok)
	require.Equal(t, string(writer.path[len(writer.path)-1]), string(os.PathSeparator))
}

func Test_WriterFileIndexAhead(t *testing.T) {
	testSetup(t, NewCBOREncoder, nil)
	defer testTeardown(t)

	fs := local.NewLocalFS(path.Join(testPath, "int-wal", defaultDatasetVersion))

	files, err := ListFiles(context.Background(), fs)
	require.NoError(t, err)

	fi := NewFileIndexFromFiles(fs, files)
	require.NotNil(t, fi)

	err = fi.AddFile(&File{
		FirstBlockNum: 13,
		LastBlockNum:  14,
	})
	require.Nil(t, err)

	err = fi.Save(context.Background())
	require.Nil(t, err)

	// test
	w, err := NewWriter[int](Options{
		Dataset: Dataset{
			Name:    "int-wal",
			Path:    testPath,
			Version: defaultDatasetVersion,
		},
	})
	defer w.Close(context.Background())
	require.NoError(t, err)

	assert.Equal(t, uint64(12), w.BlockNum())
}

func Test_WriterFileIndexBehind(t *testing.T) {
	testSetup(t, NewCBOREncoder, nil)
	defer testTeardown(t)

	fs := local.NewLocalFS(path.Join(testPath, "int-wal", defaultDatasetVersion))

	files, err := ListFiles(context.Background(), fs)
	require.NoError(t, err)

	fi := NewFileIndexFromFiles(fs, files)
	require.NotNil(t, fi)

	fi.files = fi.files[:len(fi.files)-1]

	err = fi.Save(context.Background())
	require.Nil(t, err)

	// test
	w, err := NewWriter[int](Options{
		Dataset: Dataset{
			Name:    "int-wal",
			Path:    testPath,
			Version: defaultDatasetVersion,
		},
	})
	defer w.Close(context.Background())
	require.NoError(t, err)

	assert.Equal(t, uint64(8), w.BlockNum())

	err = w.Write(context.Background(), Block[int]{Number: 11, Data: 0x0123})
	require.NoError(t, err)
	assert.Equal(t, uint64(11), w.BlockNum())

	err = w.Write(context.Background(), Block[int]{Number: 12, Data: 0x1234})
	require.NoError(t, err)
	assert.Equal(t, uint64(12), w.BlockNum())

	err = w.RollFile(context.Background())
	require.NoError(t, err)

	rdr, err := NewReader[int](Options{
		Dataset: Dataset{
			Name:    "int-wal",
			Path:    testPath,
			Version: defaultDatasetVersion,
		},
	})
	defer rdr.Close()
	require.NoError(t, err)

	err = rdr.Seek(context.Background(), 11)
	require.NoError(t, err)

	b11, err := rdr.Read(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint64(11), b11.Number)
	assert.Equal(t, 0x0123, b11.Data)

	b12, err := rdr.Read(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint64(12), b12.Number)
	assert.Equal(t, 0x1234, b12.Data)
}

// Test_WriterFileIndexSavedButFileNotWritten tests the scenario where the FileIndex is saved
// but the actual data file is not written (e.g., due to a crash between FileIndex.Save() and
// the file write completing). This can happen because in writer.writeFile(), the FileIndex is
// saved before the actual file is written.
//
// The test verifies that when a Writer is restarted after such a failure, it correctly:
// 1. Detects that the last file in the index doesn't exist on disk
// 2. Removes that entry from the FileIndex (via the logic in common.go FileIndex.readFiles())
// 3. Starts writing from the correct block number (last valid block, not the phantom file's block)
func Test_WriterFileIndexSavedButFileNotWritten(t *testing.T) {
	defer testTeardown(t)

	// Setup: Write some initial blocks and roll the file
	w, err := NewWriter[int](Options{
		Dataset: Dataset{
			Name:    "int-wal",
			Path:    testPath,
			Version: defaultDatasetVersion,
		},
		FileRollOnClose: true,
	})
	require.NoError(t, err)

	// Write blocks 1-4
	for i := uint64(1); i <= 4; i++ {
		err = w.Write(context.Background(), Block[int]{Number: i, Data: int(i * 100)})
		require.NoError(t, err)
	}

	// Roll the file to persist blocks 1-4
	w_, ok := w.(*writer[int])
	require.True(t, ok)

	err = w_.rollFile(context.Background())
	require.NoError(t, err)

	// Write blocks 5-8
	for i := uint64(5); i <= 8; i++ {
		err = w.Write(context.Background(), Block[int]{Number: i, Data: int(i * 100)})
		require.NoError(t, err)
	}

	// Close will roll the file for blocks 5-8 because FileRollOnClose=true
	err = w.Close(context.Background())
	require.NoError(t, err)

	// Simulate scenario where FileIndex is saved but file is NOT written:
	// Manually add a file entry to FileIndex without creating the actual file
	fs := local.NewLocalFS(path.Join(testPath, "int-wal", defaultDatasetVersion))

	fileIndex := NewFileIndex(fs)
	err = fileIndex.Load(context.Background())
	require.NoError(t, err)

	// Add a phantom file entry (blocks 9-12) that doesn't exist on disk
	phantomFile := &File{
		FirstBlockNum: 9,
		LastBlockNum:  12,
	}
	err = fileIndex.AddFile(phantomFile)
	require.NoError(t, err)

	// Save the FileIndex with the phantom entry
	err = fileIndex.Save(context.Background())
	require.NoError(t, err)

	// Verify that the phantom file doesn't actually exist
	require.False(t, phantomFile.Exist(context.Background(), fs))

	// Now create a new Writer - it should detect that the last file doesn't exist
	// and start from the correct position (block 8, the last valid block)
	w2, err := NewWriter[int](Options{
		Dataset: Dataset{
			Name:    "int-wal",
			Path:    testPath,
			Version: defaultDatasetVersion,
		},
	})
	require.NoError(t, err)

	// The Writer should have corrected its position to block 8 (last valid block)
	// because the phantom file (9-12) doesn't exist
	assert.Equal(t, uint64(8), w2.BlockNum(), "Writer should start from last valid block (8), not from phantom file (12)")

	// Write the next block (9) - this should succeed
	err = w2.Write(context.Background(), Block[int]{Number: 9, Data: 900})
	require.NoError(t, err)
	assert.Equal(t, uint64(9), w2.BlockNum())

	// Write more blocks
	err = w2.Write(context.Background(), Block[int]{Number: 10, Data: 1000})
	require.NoError(t, err)
	assert.Equal(t, uint64(10), w2.BlockNum())

	// Roll and close
	w2_, ok := w2.(*writer[int])
	require.True(t, ok)

	err = w2_.rollFile(context.Background())
	require.NoError(t, err)

	err = w2.Close(context.Background())
	require.NoError(t, err)

	// Verify we can read all blocks correctly
	rdr, err := NewReader[int](Options{
		Dataset: Dataset{
			Name:    "int-wal",
			Path:    testPath,
			Version: defaultDatasetVersion,
		},
	})
	require.NoError(t, err)
	defer rdr.Close()

	// Read blocks 1-10 and verify they are correct
	err = rdr.Seek(context.Background(), 1)
	require.NoError(t, err)

	for i := uint64(1); i <= 10; i++ {
		blk, err := rdr.Read(context.Background())
		require.NoError(t, err)
		assert.Equal(t, i, blk.Number)
		assert.Equal(t, int(i*100), blk.Data)
	}
}

// TestWriter_RecoverFromWriteFileError tests that the Writer can recover from an error
// in writeFile. This simulates a scenario where writeFile fails during file creation
// (after FileIndex is saved but before the actual file is written), and then the
// application is restarted. The new Writer should detect the inconsistency and recover.
func TestWriter_RecoverFromWriteFileError(t *testing.T) {
	defer testTeardown(t)

	// Create the directory structure first
	walDir := path.Join(testPath, "int-wal", defaultDatasetVersion)
	err := os.MkdirAll(walDir, 0755)
	require.NoError(t, err)

	// Create a failing filesystem that will fail on the first Create call
	failingFS := &failOnceFS{
		FS:          local.NewLocalFS(""),
		failOnCount: 1,
	}

	options := Options{
		Dataset: Dataset{
			Name:    "int-wal",
			Path:    testPath,
			Version: defaultDatasetVersion,
		},
		FileSystem: failingFS,
	}

	// Phase 1: Write blocks and encounter a failure during rollFile
	w, err := NewWriter[int](options)
	require.NoError(t, err)

	// Write some blocks
	err = w.Write(context.Background(), Block[int]{Number: 1, Data: 100})
	require.NoError(t, err)

	err = w.Write(context.Background(), Block[int]{Number: 2, Data: 200})
	require.NoError(t, err)

	// Try to roll the file - this should fail because of the failing FS
	// Note: This will partially complete writeFile (FileIndex is saved, but file creation fails)
	w_, ok := w.(*writer[int])
	require.True(t, ok)

	err = w_.rollFile(context.Background())
	require.Error(t, err, "Expected error from writeFile due to failing filesystem")
	require.Contains(t, err.Error(), "injected error")

	// Verify the writer still has the blocks in memory
	assert.Equal(t, uint64(2), w.BlockNum())

	// Close the writer (simulating application shutdown after error)
	// Don't check for error here as the state might be inconsistent
	_ = w.Close(context.Background())

	// Phase 2: Create a new Writer (simulating application restart)
	// The filesystem now works (failOnCount has been exhausted)
	// The new Writer should detect that the FileIndex references a non-existent file
	// and recover by removing that entry from the index
	w2, err := NewWriter[int](Options{
		Dataset: Dataset{
			Name:    "int-wal",
			Path:    testPath,
			Version: defaultDatasetVersion,
		},
		FileSystem: failingFS, // Same filesystem, but now it won't fail
	})
	require.NoError(t, err, "New Writer should recover from the inconsistent state")

	// The Writer should have recovered to block 0 (since no files were successfully written)
	assert.Equal(t, uint64(0), w2.BlockNum())

	// Phase 3: Write new blocks and verify everything works
	err = w2.Write(context.Background(), Block[int]{Number: 1, Data: 100})
	require.NoError(t, err)

	err = w2.Write(context.Background(), Block[int]{Number: 2, Data: 200})
	require.NoError(t, err)

	err = w2.Write(context.Background(), Block[int]{Number: 3, Data: 300})
	require.NoError(t, err)

	err = w2.Write(context.Background(), Block[int]{Number: 4, Data: 400})
	require.NoError(t, err)

	// Roll and close successfully
	w2_, ok := w2.(*writer[int])
	require.True(t, ok)

	err = w2_.rollFile(context.Background())
	require.NoError(t, err, "Should successfully write file after recovery")

	err = w2.Close(context.Background())
	require.NoError(t, err)

	// Phase 4: Verify we can read all the blocks that were successfully written
	rdr, err := NewReader[int](Options{
		Dataset: Dataset{
			Name:    "int-wal",
			Path:    testPath,
			Version: defaultDatasetVersion,
		},
	})
	require.NoError(t, err)
	defer rdr.Close()

	// Read blocks 1-4 and verify they are correct
	err = rdr.Seek(context.Background(), 1)
	require.NoError(t, err)

	for i := uint64(1); i <= 4; i++ {
		blk, err := rdr.Read(context.Background())
		require.NoError(t, err)
		assert.Equal(t, i, blk.Number)
		assert.Equal(t, int(i*100), blk.Data)
	}
}

// failOnceFS is a filesystem wrapper that fails on the Nth Create call
type failOnceFS struct {
	storage.FS
	failOnCount int
	createCount int
	mu          sync.Mutex
}

func (f *failOnceFS) Create(ctx context.Context, path string, options *gostorage.WriterOptions) (io.WriteCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.createCount++
	if f.createCount == f.failOnCount {
		return nil, fmt.Errorf("injected error on Create call #%d for path: %s", f.createCount, path)
	}

	return f.FS.Create(ctx, path, options)
}

func BenchmarkWriter_Write(b *testing.B) {
	defer func() {
		_ = os.RemoveAll(testPath)
	}()

	testCase := []struct {
		name    string
		options Options
	}{
		{
			name: "json",
			options: Options{
				Dataset: Dataset{
					Name:    "int-wal",
					Path:    testPath,
					Version: defaultDatasetVersion,
				},
				NewEncoder: NewJSONEncoder,
				NewDecoder: NewJSONDecoder,
			},
		},
		{
			name: "json-zstd",
			options: Options{
				Dataset: Dataset{
					Name:    "int-wal",
					Path:    testPath,
					Version: defaultDatasetVersion,
				},
				NewEncoder:      NewJSONEncoder,
				NewDecoder:      NewJSONDecoder,
				NewCompressor:   NewZSTDCompressor,
				NewDecompressor: NewZSTDDecompressor,
			},
		},
		{
			name: "cbor",
			options: Options{
				Dataset: Dataset{
					Name:    "int-wal",
					Path:    testPath,
					Version: defaultDatasetVersion,
				},
				NewEncoder: NewCBOREncoder,
				NewDecoder: NewCBORDecoder,
			},
		},
		{
			name: "cbor-zstd",
			options: Options{
				Dataset: Dataset{
					Name:    "int-wal",
					Path:    testPath,
					Version: defaultDatasetVersion,
				},
				NewEncoder:      NewCBOREncoder,
				NewDecoder:      NewCBORDecoder,
				NewCompressor:   NewZSTDCompressor,
				NewDecompressor: NewZSTDDecompressor,
			},
		},
	}

	for _, tc := range testCase {
		b.Run(tc.name, func(b *testing.B) {
			w, err := NewWriter[int](tc.options)
			require.NoError(b, err)

			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				for j := 0; j < 1000000; j++ {
					err := w.Write(context.Background(), Block[int]{Number: uint64(i)})
					require.NoError(b, err)
				}
			}

			err = w.Close(context.Background())
			require.NoError(b, err)
		})
	}
}
