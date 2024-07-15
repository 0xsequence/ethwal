package ethwal

import (
	"context"
	"fmt"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/0xsequence/ethwal/storage"
	"github.com/0xsequence/ethwal/storage/local"
)

type WALFile struct {
	Name          string
	FirstBlockNum uint64
	LastBlockNum  uint64
}

type Dataset struct {
	Name      string
	Version   string
	Path      string
	CachePath string
}

func (d Dataset) FullPath() string {
	return buildETHWALPath(d.Name, d.Version, d.Path)
}

func (d Dataset) FullCachePath() string {
	return buildETHWALPath(d.Name, d.Version, d.CachePath)
}

type Options struct {
	Dataset Dataset

	FileSystem storage.FS

	NewCompressor   NewCompressorFunc
	NewDecompressor NewDecompressorFunc

	NewEncoder NewEncoderFunc
	NewDecoder NewDecoderFunc

	FileRollPolicy  FileRollPolicy
	FileRollOnClose bool
}

func (o Options) WithDefaults() Options {
	if o.FileSystem == nil {
		wd, _ := os.Getwd()
		o.FileSystem = local.NewLocalFS(wd)
	}
	if o.NewEncoder == nil {
		o.NewEncoder = NewCBOREncoder
	}
	if o.NewDecoder == nil {
		o.NewDecoder = NewCBORDecoder
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
// The path is built as follows: <walPath>/<name?>/<version?>
func buildETHWALPath(name, version, walPath string) string {
	var parts = []string{walPath}

	if name != "" {
		parts = append(parts, name)
	}

	if version != "" {
		parts = append(parts, version)
	}

	return path.Join(walPath, name, version)
}

// ParseWALFileBlockRange reads first and last block number stored in WAL file from file name
func ParseWALFileBlockRange(filePath string) (uint64, uint64) {
	_, fileName := path.Split(filePath)
	fileNameSplit := strings.Split(fileName, ".")
	blockNumberSplit := strings.Split(fileNameSplit[0], "_")

	first, _ := strconv.ParseInt(blockNumberSplit[0], 10, 64)
	last, _ := strconv.ParseInt(blockNumberSplit[1], 10, 64)

	return uint64(first), uint64(last)
}

// ListWALFiles lists all WAL files in the provided file system
func ListWALFiles(fs storage.FS) ([]WALFile, error) {
	wlk, ok := fs.(storage.Walker)
	if !ok {
		return nil, fmt.Errorf("ethlogwal: provided file system does not implement Walker interface")
	}

	var walFiles []WALFile
	err := wlk.Walk(context.Background(), "", func(filePath string) error {
		// walk only wal files
		if path.Ext(filePath) != ".wal" {
			return nil
		}

		_, fileName := path.Split(filePath)
		firstBlockNum, lastBlockNum := ParseWALFileBlockRange(fileName)
		walFiles = append(walFiles, WALFile{
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
