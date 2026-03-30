package ethwal

import (
	"encoding/json"
	"io"

	"github.com/fxamacker/cbor/v2"
)

type NewEncoderFunc func(w io.Writer) Encoder
type NewDecoderFunc func(r io.Reader) Decoder

type Encoder interface {
	Encode(v any) error
}

type Decoder interface {
	Decode(v any) error
}

func NewJSONEncoder(w io.Writer) Encoder {
	return json.NewEncoder(w)
}

func NewJSONDecoder(r io.Reader) Decoder {
	return json.NewDecoder(r)
}

func NewCBOREncoder(w io.Writer) Encoder {
	return cbor.NewEncoder(w)
}

// Single ceiling for CBOR arrays and maps (fxamacker/cbor defaults are 131072 each).
const cborMaxDecodeElements = 500_000

type errDecoder struct{ err error }

func (d errDecoder) Decode(v any) error { return d.err }

func NewCBORDecoder(r io.Reader) Decoder {
	opt := cbor.DecOptions{
		MaxNestedLevels:  1024,
		MaxArrayElements: cborMaxDecodeElements,
		MaxMapPairs:      cborMaxDecodeElements,
	}
	mode, err := opt.DecMode()
	if err != nil {
		return errDecoder{err}
	}
	return mode.NewDecoder(r)
}
