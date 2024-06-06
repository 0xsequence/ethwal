package ethlogwal

import (
	"io"

	"github.com/DataDog/zstd"
)

type Compressor interface {
	io.WriteCloser
}

type Decompressor interface {
	io.ReadCloser
}

type NewCompressorFunc func(w io.Writer) Compressor

type NewDecompressorFunc func(r io.Reader) Decompressor

func NewZSTDCompressor(w io.Writer) Compressor {
	return zstd.NewWriterLevel(w, zstd.BestSpeed)
}

func NewZSTDDecompressor(r io.Reader) Decompressor {
	return zstd.NewReader(r)
}
