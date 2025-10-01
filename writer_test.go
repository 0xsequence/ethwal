package ethwal

import (
	"context"
	"io"
	"os"
	"path"
	"testing"

	"github.com/0xsequence/ethkit/go-ethereum/common"
	"github.com/0xsequence/ethwal/storage/local"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriter_Write(t *testing.T) {
	blocksFile := Blocks[int]{
		{
			Hash:   common.BytesToHash([]byte{0x00}),
			Number: 0,
			TS:     0,
			Data:   0,
		},
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
			filePath := path.Join(buildETHWALPath(tc.options.Dataset.Name, tc.options.Dataset.Version, tc.options.Dataset.Path), (&File{FirstBlockNum: 0, LastBlockNum: 4}).Path())
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

func TestWriter_Write_ZeroBlockNum(t *testing.T) {
	defer testTeardown(t)

	w, err := NewWriter[int](Options{
		Dataset: Dataset{
			Name:    "int-wal",
			Path:    testPath,
			Version: defaultDatasetVersion,
		},
	})
	require.NoError(t, err)

	err = w.Write(context.Background(), Block[int]{Number: 0, Hash: common.BytesToHash([]byte{0x01}), Parent: common.Hash{0x00}})
	require.NoError(t, err)

	err = w.Write(context.Background(), Block[int]{Number: 1, Hash: common.BytesToHash([]byte{0x02}), Parent: common.BytesToHash([]byte{0x01})})
	require.NoError(t, err)

	err = w.Write(context.Background(), Block[int]{Number: 2, Hash: common.BytesToHash([]byte{0x03}), Parent: common.BytesToHash([]byte{0x02})})
	require.NoError(t, err)

	err = w.RollFile(context.Background())
	require.NoError(t, err)

	err = w.Write(context.Background(), Block[int]{Number: 3, Hash: common.BytesToHash([]byte{0x04}), Parent: common.BytesToHash([]byte{0x03})})
	require.NoError(t, err)

	err = w.Write(context.Background(), Block[int]{Number: 4, Hash: common.BytesToHash([]byte{0x05}), Parent: common.BytesToHash([]byte{0x04})})
	require.NoError(t, err)

	err = w.Write(context.Background(), Block[int]{Number: 5, Hash: common.BytesToHash([]byte{0x06}), Parent: common.BytesToHash([]byte{0x05})})
	require.NoError(t, err)

	err = w.RollFile(context.Background())
	require.NoError(t, err)

	err = w.Close(context.Background())
	require.NoError(t, err)

	// check WAL files
	r, err := NewReader[int](Options{
		Dataset: Dataset{
			Name:    "int-wal",
			Path:    testPath,
			Version: defaultDatasetVersion,
		},
	})
	require.NoError(t, err)

	err = r.Seek(context.Background(), 0)
	require.NoError(t, err)

	for i := 0; i < 6; i++ {
		blk, err := r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, uint64(i), blk.Number)
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

	err = ngw.Write(context.Background(), Block[int]{Number: 0})
	require.NoError(t, err)

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
	filePath := path.Join(buildETHWALPath(opt.Dataset.Name, opt.Dataset.Version, opt.Dataset.Path), (&File{FirstBlockNum: 0, LastBlockNum: 3}).Path())
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
