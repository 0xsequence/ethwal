package ethlogwal

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

func NewCBORDecoder(r io.Reader) Decoder {
	return cbor.NewDecoder(r)
}
