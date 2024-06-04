package ethlogwal

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/Shopify/go-storage"
	"github.com/fxamacker/cbor/v2"
)

var (
	WALFormatVersion = "v4"
)

type walFile struct {
	Name          string
	FirstBlockNum uint64
	LastBlockNum  uint64
}

type Options struct {
	Name            string
	Path            string
	CachePath       string
	MaxWALSize      uint64
	UseCompression  bool
	UseJSONEncoding bool

	GoogleCloudStorageBucket string
}

type fileStats struct {
	io.Writer
	BytesWritten uint64
}

func (w *fileStats) Write(p []byte) (n int, err error) {
	n, err = w.Writer.Write(p)
	w.BytesWritten += uint64(n)
	return
}

// parseWALFileBlockRange reads first and last block number stored in WAL file from file name
func parseWALFileBlockRange(filePath string) (uint64, uint64) {
	_, fileName := path.Split(filePath)
	fileNameSplit := strings.Split(fileName, ".")
	blockNumberSplit := strings.Split(fileNameSplit[0], "_")

	first, _ := strconv.ParseInt(blockNumberSplit[0], 10, 64)
	last, _ := strconv.ParseInt(blockNumberSplit[1], 10, 64)

	return uint64(first), uint64(last)
}

func listWALFiles(fs storage.FS) ([]walFile, error) {
	wlk, ok := fs.(storage.Walker)
	if !ok {
		return nil, fmt.Errorf("ethlogwal: provided file system does not implement Walker interface")
	}

	var walFiles []walFile
	err := wlk.Walk(context.Background(), "", func(filePath string) error {
		// walk only wal files
		if path.Ext(filePath) != ".wal" {
			return nil
		}

		_, fileName := path.Split(filePath)
		firstBlockNum, lastBlockNum := parseWALFileBlockRange(fileName)
		walFiles = append(walFiles, walFile{
			Name:          fileName,
			FirstBlockNum: firstBlockNum,
			LastBlockNum:  lastBlockNum,
		})
		return nil
	})
	if err != nil {
		return nil, err
	}

	sort.Slice(walFiles, func(i, j int) bool {
		return walFiles[i].FirstBlockNum < walFiles[j].FirstBlockNum
	})

	return walFiles, nil
}

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

type funcCloser struct {
	CloseFunc func() error
}

func (f *funcCloser) Close() error {
	if f.CloseFunc != nil {
		return f.CloseFunc()
	}
	return nil
}
