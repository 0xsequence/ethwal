package ethlogwal

import (
	"io"
	"os"
	"path"
	"testing"

	"github.com/0xsequence/go-sequence/lib/prototyp"
	"github.com/stretchr/testify/require"
)

// TODO: write generic tests with diffrent encoders / decoders and compressors

func TestWriter_Write(t *testing.T) {
	blocksFile := Blocks[int]{
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

	testCase := []struct {
		name    string
		options Options
	}{
		{
			name: "json",
			options: Options{
				Name:       "int-wal",
				Path:       testPath,
				NewEncoder: NewJSONEncoder,
				NewDecoder: NewJSONDecoder,
			},
		},
		{
			name: "json-zstd",
			options: Options{
				Name:            "int-wal",
				Path:            testPath,
				NewEncoder:      NewJSONEncoder,
				NewDecoder:      NewJSONDecoder,
				NewCompressor:   NewZSTDCompressor,
				NewDecompressor: NewZSTDDecompressor,
			},
		},
		{
			name: "cbor",
			options: Options{
				Name:       "int-wal",
				Path:       testPath,
				NewEncoder: NewCBOREncoder,
				NewDecoder: NewCBORDecoder,
			},
		},
		{
			name: "cbor-zstd",
			options: Options{
				Name:            "int-wal",
				Path:            testPath,
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

			w, err := NewWriter[int](tc.options)
			require.NoError(t, err)

			for _, block := range blocksFile {
				err := w.Write(block)
				require.NoError(t, err)
			}

			// flush the in-memory buffer to disk
			w_, ok := w.(*writer[int])
			require.True(t, ok)

			err = w_.rollFile()
			require.NoError(t, err)

			err = w.Close()
			require.NoError(t, err)

			// check WAL files
			filePath := path.Join(buildETHWALPath(tc.options.Name, tc.options.Path), "1_4.wal")
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

func Test_WriterStoragePathSuffix(t *testing.T) {
	options := Options{
		Name: "int-wal",
		Path: testPath,
	}

	w, err := NewWriter[int](options)
	require.NoError(t, err)

	writer, ok := w.(*writer[int])
	require.True(t, ok)
	require.Equal(t, string(writer.path[len(writer.path)-1]), string(os.PathSeparator))
}
