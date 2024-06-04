package ethlogwal

import (
	"fmt"
	"io"
	"os"
	"path"
	"testing"

	"github.com/0xsequence/ethkit/go-ethereum/common"
	"github.com/0xsequence/ethkit/go-ethereum/core/types"
	"github.com/DataDog/zstd"
	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"
)

func TestWriter_Write(t *testing.T) {
	defer testTeardown(t)

	blocksFile := Blocks[[]types.Log]{
		{
			BlockHash:   common.Hash{0x01},
			BlockNumber: 1,
			TS:          0,
			Data:        nil,
		},
		{
			BlockHash:   common.Hash{0x02},
			BlockNumber: 2,
			TS:          0,
			Data:        nil,
		},
		{
			BlockHash:   common.Hash{0x03},
			BlockNumber: 3,
			TS:          0,
			Data:        nil,
		},
		{
			BlockHash:   common.Hash{0x04},
			BlockNumber: 4,
			TS:          0,
			Data:        nil,
		},
	}

	options := Options{
		Path:           testPath,
		MaxWALSize:     datasize.MB.Bytes(),
		UseCompression: true,
		// UseJSONEncoding: true,
	}

	w, err := NewWriter[[]types.Log](options)
	require.NoError(t, err)

	for _, block := range blocksFile {
		err := w.Write(block)
		require.NoError(t, err)
	}

	// flush the in-memory buffer to disk
	w_, ok := w.(*writer[[]types.Log])
	require.True(t, ok)
	w_.writeNextFile()

	err = w.Close()
	require.NoError(t, err)

	// check WAL files
	filePath := path.Join(testPath, fmt.Sprintf("%v", WALFormatVersion), "1_4.wal")
	_, err = os.Stat(filePath)
	require.NoError(t, err)

	f, err := os.Open(filePath)
	require.NoError(t, err)

	var r io.Reader
	if options.UseCompression {
		r = zstd.NewReader(f)
	} else {
		r = f
	}

	var dec Decoder
	if options.UseJSONEncoding {
		dec = newJSONDecoder(r)
	} else {
		dec = newBinaryDecoder(r)
	}

	var blocks Blocks[[]types.Log]
	for {
		var block Block[[]types.Log]
		err := dec.Decode(&block)
		if err != nil {
			break
		}
		blocks = append(blocks, block)
	}

	require.Equal(t, blocksFile, blocks)
}

func Test_WriterStoragePathSuffix(t *testing.T) {
	options := Options{
		Path:           testPath,
		MaxWALSize:     datasize.MB.Bytes(),
		UseCompression: true,

		// UseJSONEncoding: true,
	}

	w, err := NewWriter[[]types.Log](options)
	require.NoError(t, err)

	writer, ok := w.(*writer[[]types.Log])
	require.True(t, ok)
	require.Equal(t, string(writer.path[len(writer.path)-1]), string(os.PathSeparator))
}
