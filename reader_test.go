package ethlogwal

import (
	"io"
	"os"
	"path"
	"testing"

	"github.com/0xsequence/ethkit/go-ethereum/core/types"
	"github.com/0xsequence/go-sequence/lib/prototyp"
	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testPath = ".tmp/ethwal"

func testSetup(t *testing.T) {
	blocksFile1 := Blocks[int]{
		{
			Hash:   prototyp.HashFromBytes([]byte{0x01}),
			Number: 1,
			TS:     0,
			Data:   0,
		},
		{
			Hash:   prototyp.HashFromBytes([]byte{0x02}),
			Number: 2,
			TS:     0,
			Data:   0,
		},
		{
			Hash:   prototyp.HashFromBytes([]byte{0x03}),
			Number: 3,
			TS:     0,
			Data:   0,
		},
		{
			Hash:   prototyp.HashFromBytes([]byte{0x04}),
			Number: 4,
			TS:     0,
			Data:   0,
		},
	}

	blocksFile2 := Blocks[int]{
		{
			Hash:   prototyp.HashFromBytes([]byte{0x05}),
			Number: 5,
			TS:     0,
			Data:   0,
		},
		{
			Hash:   prototyp.HashFromBytes([]byte{0x06}),
			Number: 6,
			TS:     0,
			Data:   0,
		},
		{
			Hash:   prototyp.HashFromBytes([]byte{0x07}),
			Number: 7,
			TS:     0,
			Data:   0,
		},
		{
			Hash:   prototyp.HashFromBytes([]byte{0x08}),
			Number: 8,
			TS:     0,
			Data:   0,
		},
	}

	blocksFile3 := Blocks[int]{
		{
			Hash:   prototyp.HashFromBytes([]byte{0x0b}),
			Number: 11,
			TS:     0,
			Data:   0,
		},
		{
			Hash:   prototyp.HashFromBytes([]byte{0x0c}),
			Number: 12,
			TS:     0,
			Data:   0,
		},
	}

	walDir := path.Join(testPath, "int-wal", WALFormatVersion)
	_ = os.MkdirAll(walDir, 0755)

	f, err := os.OpenFile(path.Join(walDir, "1_4.wal"), os.O_CREATE|os.O_WRONLY, 0755)
	require.NoError(t, err)

	enc := newBinaryEncoder(f)
	for _, blk := range blocksFile1 {
		_ = enc.Encode(blk)
	}
	_ = f.Close()

	f, err = os.OpenFile(path.Join(walDir, "5_8.wal"), os.O_CREATE|os.O_WRONLY, 0755)
	require.NoError(t, err)

	enc = newBinaryEncoder(f)
	for _, blk := range blocksFile2 {
		_ = enc.Encode(blk)
	}
	_ = f.Close()

	f, err = os.OpenFile(path.Join(walDir, "11_12.wal"), os.O_CREATE|os.O_WRONLY, 0755)
	require.NoError(t, err)

	enc = newBinaryEncoder(f)
	for _, blk := range blocksFile3 {
		_ = enc.Encode(blk)
	}
	_ = f.Close()
}

func testTeardown(t *testing.T) {
	_ = os.RemoveAll(testPath)
}

func TestReader_All(t *testing.T) {
	testSetup(t)
	defer testTeardown(t)

	rdr, err := NewReader[int](Options{
		Name:           "int-wal",
		Path:           testPath,
		MaxWALSize:     datasize.MB.Bytes(),
		UseCompression: false,
	})
	require.NoError(t, err)

	var blk Block[int]
	var blks []Block[int]
	for blk, err = rdr.Read(); err == nil; blk, err = rdr.Read() {
		t.Logf("blk: %+v", blk)
		blks = append(blks, blk)
	}

	require.Equal(t, io.EOF, err)
	assert.Equal(t, 10, len(blks))
}

func TestReader_Seek(t *testing.T) {
	testSetup(t)
	defer testTeardown(t)

	rdr, err := NewReader[int](Options{
		Name:           "int-wal",
		Path:           testPath,
		MaxWALSize:     datasize.MB.Bytes(),
		UseCompression: false,
	})
	require.NoError(t, err)

	// seek to 2
	err = rdr.Seek(2)
	require.NoError(t, err)

	blk, err := rdr.Read()
	require.NoError(t, err)
	assert.Equal(t, uint64(2), blk.Number)

	blk, err = rdr.Read()
	require.NoError(t, err)
	assert.Equal(t, uint64(3), blk.Number)

	// seek to 10, which does not exist but there is a file with block 11
	err = rdr.Seek(10)
	require.NoError(t, err)

	blk, err = rdr.Read()
	require.NoError(t, err)
	assert.Equal(t, uint64(11), blk.Number)

	blk, err = rdr.Read()
	require.NoError(t, err)
	assert.Equal(t, uint64(12), blk.Number)

	_, err = rdr.Read()
	require.Equal(t, io.EOF, err)

	//  reader should return EOF on consecutive reads
	_, err = rdr.Read()
	require.Equal(t, io.EOF, err)

	// seek to 50 which does not exist and there is no file with block 50 or higher
	err = rdr.Seek(50)
	require.Equal(t, io.EOF, err)
}

func Test_ReaderStoragePathSuffix(t *testing.T) {
	options := Options{
		Name:           "int-wal",
		Path:           testPath,
		MaxWALSize:     datasize.MB.Bytes(),
		UseCompression: true,
		// UseJSONEncoding: true,
	}

	r, err := NewReader[[]types.Log](options)
	require.NoError(t, err)
	reader, ok := r.(*reader[[]types.Log])
	require.True(t, ok)
	require.Equal(t, string(reader.path[len(reader.path)-1]), string(os.PathSeparator))
}
