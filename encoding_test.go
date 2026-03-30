package ethwal

import (
	"bytes"
	"testing"

	"github.com/fxamacker/cbor/v2"
	"github.com/stretchr/testify/require"
)

func TestNewCBORDecoderLargeArray(t *testing.T) {
	const n = 131073
	s := make([]uint64, n)
	for i := range s {
		s[i] = uint64(i)
	}
	data, err := cbor.Marshal(s)
	require.NoError(t, err)

	var out []uint64
	dec := NewCBORDecoder(bytes.NewReader(data))
	err = dec.Decode(&out)
	require.NoError(t, err)
	require.Len(t, out, n)
}

func TestNewCBORDecoderLargeMap(t *testing.T) {
	const n = 131073
	m := make(map[uint64]uint64, n)
	for i := uint64(0); i < n; i++ {
		m[i] = i
	}
	data, err := cbor.Marshal(m)
	require.NoError(t, err)

	var out map[uint64]uint64
	dec := NewCBORDecoder(bytes.NewReader(data))
	err = dec.Decode(&out)
	require.NoError(t, err)
	require.Len(t, out, n)
	require.Equal(t, uint64(0), out[0])
	require.Equal(t, uint64(n-1), out[n-1])
	require.Equal(t, uint64(n/2), out[n/2])
}
