package ethlogwal

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/Shopify/go-storage"
)

var (
	WALFormatVersion = "v5"
)

type walFile struct {
	Name          string
	FirstBlockNum uint64
	LastBlockNum  uint64
}

type Options struct {
	Name string

	Path      string
	CachePath string

	NewCompressor   NewCompressorFunc
	NewDecompressor NewDecompressorFunc

	NewEncoder NewEncoderFunc
	NewDecoder NewDecoderFunc

	FileRollPolicy FileRollPolicy

	GoogleCloudStorageBucket string
}

func (o Options) WithDefaults() Options {
	if o.NewEncoder == nil {
		o.NewEncoder = NewBinaryEncoder
	}
	if o.NewDecoder == nil {
		o.NewDecoder = NewBinaryDecoder
	}
	if o.FileRollPolicy == nil {
		o.FileRollPolicy = NewFileSizeRollPolicy(uint64(defaultBufferSize))
	}
	return o
}

// funcCloser is a helper struct that implements io.Closer interface
type funcCloser struct {
	CloseFunc func() error
}

func (f *funcCloser) Close() error {
	if f.CloseFunc != nil {
		return f.CloseFunc()
	}
	return nil
}

// buildETHWALPath returns the path to the WAL directory
func buildETHWALPath(name, p string) string {
	return path.Join(p, name, WALFormatVersion)
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

// listWALFiles lists all WAL files in the provided file system
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
