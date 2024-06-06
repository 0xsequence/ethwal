package ethlogwal

import (
	"encoding/json"
	"io"

	"github.com/fxamacker/cbor/v2"
)

type Encoder interface {
	Encode(v any) error
}

type Decoder interface {
	Decode(v any) error
}

func newJSONEncoder(w io.Writer) *json.Encoder {
	return json.NewEncoder(w)
}

func newJSONDecoder(r io.Reader) *json.Decoder {
	return json.NewDecoder(r)
}

func newBinaryEncoder(w io.Writer) *cbor.Encoder {
	return cbor.NewEncoder(w)
}

func newBinaryDecoder(r io.Reader) *cbor.Decoder {
	return cbor.NewDecoder(r)
}
