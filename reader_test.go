package ethwal

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"testing"

	"github.com/0xsequence/ethkit/go-ethereum/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const defaultDatasetVersion = "v1"

const testRoot = ".tmp"
const testPath = ".tmp/ethwal"

func testSetup(t *testing.T, newEncoder NewEncoderFunc, newCompressor NewCompressorFunc) {
	blocksFile1 := Blocks[int]{
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

	blocksFile2 := Blocks[int]{
		{
			Hash:   common.BytesToHash([]byte{0x05}),
			Number: 5,
			TS:     0,
			Data:   0,
		},
		{
			Hash:   common.BytesToHash([]byte{0x06}),
			Number: 6,
			TS:     0,
			Data:   0,
		},
		{
			Hash:   common.BytesToHash([]byte{0x07}),
			Number: 7,
			TS:     0,
			Data:   0,
		},
		{
			Hash:   common.BytesToHash([]byte{0x08}),
			Number: 8,
			TS:     0,
			Data:   0,
		},
	}

	blocksFile3 := Blocks[int]{
		{
			Hash:   common.BytesToHash([]byte{0x0b}),
			Number: 11,
			TS:     0,
			Data:   0,
		},
		{
			Hash:   common.BytesToHash([]byte{0x0c}),
			Number: 12,
			TS:     0,
			Data:   0,
		},
	}

	walDir := path.Join(testPath, "int-wal", defaultDatasetVersion)
	_ = os.MkdirAll(walDir, 0755)

	f, err := os.OpenFile(path.Join(walDir, "1_4.wal"), os.O_CREATE|os.O_WRONLY, 0755)
	require.NoError(t, err)

	var w io.WriteCloser = f
	if newCompressor != nil {
		w = newCompressor(f)
	}

	enc := newEncoder(w)
	for _, blk := range blocksFile1 {
		_ = enc.Encode(blk)
	}
	_ = w.Close()

	f, err = os.OpenFile(path.Join(walDir, "5_8.wal"), os.O_CREATE|os.O_WRONLY, 0755)
	require.NoError(t, err)

	w = f
	if newCompressor != nil {
		w = newCompressor(f)
	}

	enc = newEncoder(w)
	for _, blk := range blocksFile2 {
		_ = enc.Encode(blk)
	}
	_ = w.Close()

	f, err = os.OpenFile(path.Join(walDir, "11_12.wal"), os.O_CREATE|os.O_WRONLY, 0755)
	require.NoError(t, err)

	w = f
	if newCompressor != nil {
		w = newCompressor(f)
	}

	enc = newEncoder(w)
	for _, blk := range blocksFile3 {
		_ = enc.Encode(blk)
	}
	_ = w.Close()
}

func testTeardown(t *testing.T) {
	fmt.Println("teardown")
	_ = os.RemoveAll(testRoot)
}

func TestReader_Read(t *testing.T) {
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
			testSetup(t, tc.options.NewEncoder, tc.options.NewCompressor)
			defer testTeardown(t)

			rdr, err := NewReader[int](tc.options)
			require.NoError(t, err)

			var blk Block[int]
			var blks []Block[int]
			for blk, err = rdr.Read(context.Background()); err == nil; blk, err = rdr.Read(context.Background()) {
				//t.Logf("blk: %+v", blk)
				blks = append(blks, blk)
			}

			require.Equal(t, io.EOF, err)
			assert.Equal(t, 10, len(blks))

			assert.NoError(t, rdr.Close())
		})
	}
}

func TestReader_NumWALFiles(t *testing.T) {
	testSetup(t, NewCBOREncoder, nil)
	defer testTeardown(t)

	rdr, err := NewReader[int](Options{
		Dataset: Dataset{
			Name:    "int-wal",
			Path:    testPath,
			Version: defaultDatasetVersion,
		},
		NewEncoder: NewCBOREncoder,
		NewDecoder: NewCBORDecoder,
	})
	require.NoError(t, err)

	assert.Equal(t, 3, rdr.FilesNum())

	require.NoError(t, rdr.Close())
}

func TestReader_BlockNum(t *testing.T) {
	testSetup(t, NewCBOREncoder, nil)
	defer testTeardown(t)

	rdr, err := NewReader[int](Options{
		Dataset: Dataset{
			Name:    "int-wal",
			Path:    testPath,
			Version: defaultDatasetVersion,
		},
		NewEncoder: NewCBOREncoder,
		NewDecoder: NewCBORDecoder,
	})
	require.NoError(t, err)

	assert.Equal(t, uint64(0), rdr.BlockNum())

	blk, err := rdr.Read(context.Background())
	require.NoError(t, err)

	assert.Equal(t, uint64(1), blk.Number)
	assert.Equal(t, uint64(1), rdr.BlockNum())

	blk, err = rdr.Read(context.Background())
	require.NoError(t, err)

	assert.Equal(t, uint64(2), blk.Number)
	assert.Equal(t, uint64(2), rdr.BlockNum())

	err = rdr.Seek(context.Background(), 5)
	require.NoError(t, err)
	assert.Equal(t, uint64(4), rdr.BlockNum()) // last block read was 4 next block is 5

	blk, err = rdr.Read(context.Background())
	require.NoError(t, err)

	assert.Equal(t, uint64(5), blk.Number)
	assert.Equal(t, uint64(5), rdr.BlockNum())

	require.NoError(t, rdr.Close())
}

func TestReader_Seek(t *testing.T) {
	testSetup(t, NewCBOREncoder, nil)
	defer testTeardown(t)

	rdr, err := NewReader[int](Options{
		Dataset: Dataset{
			Name:    "int-wal",
			Path:    testPath,
			Version: defaultDatasetVersion,
		},
		NewEncoder: NewCBOREncoder,
		NewDecoder: NewCBORDecoder,
	})
	require.NoError(t, err)

	// seek to 2
	err = rdr.Seek(context.Background(), 2)
	require.NoError(t, err)

	blk, err := rdr.Read(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint64(2), blk.Number)

	blk, err = rdr.Read(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint64(3), blk.Number)

	// seek to 10, which does not exist but there is a file with block 11
	err = rdr.Seek(context.Background(), 10)
	require.NoError(t, err)

	blk, err = rdr.Read(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint64(11), blk.Number)

	blk, err = rdr.Read(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint64(12), blk.Number)

	_, err = rdr.Read(context.Background())
	require.Equal(t, io.EOF, err)

	//  reader should return EOF on consecutive reads
	_, err = rdr.Read(context.Background())
	require.Equal(t, io.EOF, err)

	// seek to 50 which does not exist and there is no file with block 50 or higher
	err = rdr.Seek(context.Background(), 50)
	require.Equal(t, io.EOF, err)
}

func Test_ReaderStoragePathSuffix(t *testing.T) {
	defer testTeardown(t)

	options := Options{
		Dataset: Dataset{
			Name:    "int-wal",
			Path:    testPath,
			Version: defaultDatasetVersion,
		},
	}

	r, err := NewReader[int](options)
	require.NoError(t, err)
	reader, ok := r.(*reader[int])
	require.True(t, ok)
	require.Equal(t, string(reader.path[len(reader.path)-1]), string(os.PathSeparator))
}

func Test_ReaderFileIndexAhead(t *testing.T) {
	t.Skip()
}
