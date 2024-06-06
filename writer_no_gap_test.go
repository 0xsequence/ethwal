package ethlogwal

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriterNoGap(t *testing.T) {
	t.Run("nogap", func(t *testing.T) {
		defer os.RemoveAll("wal")

		w, err := NewWriter[int](Options{
			Name:       "int-wal",
			Path:       "wal",
			NewEncoder: NewJSONEncoder,
		})
		require.NoError(t, err)

		ngw := NewWriterNoGap[int](w)
		require.NotNil(t, w)

		err = ngw.Write(Block[int]{Number: 1})
		require.NoError(t, err)

		err = ngw.Write(Block[int]{Number: 2})
		require.NoError(t, err)

		err = ngw.Write(Block[int]{Number: 3})
		require.NoError(t, err)

		err = (w.(*writer[int])).rollFile()
		require.NoError(t, err)

		err = ngw.Close()
		require.NoError(t, err)

		walData, err := os.ReadFile("wal/int-wal/v5/1_3.wal")
		require.NoError(t, err)

		d := NewJSONDecoder(bytes.NewBuffer(walData))

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
			Name:       "int-wal",
			Path:       "wal",
			NewEncoder: NewJSONEncoder,
		})
		require.NoError(t, err)

		ngw := NewWriterNoGap[int](w)
		require.NotNil(t, w)

		err = ngw.Write(Block[int]{Number: 1})
		require.NoError(t, err)

		err = ngw.Write(Block[int]{Number: 2})
		require.NoError(t, err)

		err = ngw.Write(Block[int]{Number: 3})
		require.NoError(t, err)

		err = ngw.Write(Block[int]{Number: 10})

		err = (w.(*writer[int])).rollFile()
		require.NoError(t, err)

		err = ngw.Close()
		require.NoError(t, err)

		walData, err := os.ReadFile("wal/int-wal/v5/1_10.wal")
		require.NoError(t, err)

		d := NewJSONDecoder(bytes.NewBuffer(walData))

		var b Block[int]
		var blockCount int
		for d.Decode(&b) != io.EOF {
			require.NoError(t, err)
			blockCount++
		}

		require.Equal(t, 10, blockCount)
	})
}