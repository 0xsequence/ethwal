package ethlogwal

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"
)

func TestWriterNoGap(t *testing.T) {
	t.Run("nogap", func(t *testing.T) {
		defer os.RemoveAll("wal")

		w, err := NewWriter[int](Options{
			Name:            "int-wal",
			Path:            "wal",
			MaxWALSize:      uint64(1 * datasize.MB),
			UseCompression:  false,
			UseJSONEncoding: true,
		})
		require.NoError(t, err)

		ngw := NewWriterNoGap[int](w)
		require.NotNil(t, w)

		err = ngw.Write(Block[int]{BlockNumber: 1})
		require.NoError(t, err)

		err = ngw.Write(Block[int]{BlockNumber: 2})
		require.NoError(t, err)

		err = ngw.Write(Block[int]{BlockNumber: 3})
		require.NoError(t, err)

		err = (w.(*writer[int])).writeNextFile()
		require.NoError(t, err)

		err = ngw.Close()
		require.NoError(t, err)

		walData, err := os.ReadFile("wal/int-wal/v4/1_3.wal")
		require.NoError(t, err)

		d := newJSONDecoder(bytes.NewBuffer(walData))

		var b Block[int]
		var blockCount int
		for d.Decode(&b) != io.EOF {
			require.NoError(t, err)
			blockCount++
		}

		require.Equal(t, 3, blockCount)
	})

	t.Run("gap_3_10", func(t *testing.T) {
		defer os.RemoveAll("wal")

		w, err := NewWriter[int](Options{
			Name:            "int-wal",
			Path:            "wal",
			MaxWALSize:      uint64(1 * datasize.MB),
			UseCompression:  false,
			UseJSONEncoding: true,
		})
		require.NoError(t, err)

		ngw := NewWriterNoGap[int](w)
		require.NotNil(t, w)

		err = ngw.Write(Block[int]{BlockNumber: 1})
		require.NoError(t, err)

		err = ngw.Write(Block[int]{BlockNumber: 2})
		require.NoError(t, err)

		err = ngw.Write(Block[int]{BlockNumber: 3})
		require.NoError(t, err)

		err = ngw.Write(Block[int]{BlockNumber: 10})

		err = (w.(*writer[int])).writeNextFile()
		require.NoError(t, err)

		err = ngw.Close()
		require.NoError(t, err)

		walData, err := os.ReadFile("wal/int-wal/v4/1_10.wal")
		require.NoError(t, err)

		d := newJSONDecoder(bytes.NewBuffer(walData))

		var b Block[int]
		var blockCount int
		for d.Decode(&b) != io.EOF {
			require.NoError(t, err)
			blockCount++
		}

		require.Equal(t, 10, blockCount)
	})
}
