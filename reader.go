package ethlogwal

import (
	"context"
	"errors"
	"ethwal/storage/gcloud"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"

	"github.com/Shopify/go-storage"
	"github.com/fatih/structs"
)

const firstFileIndex = 0

type Reader[T any] interface {
	NumWALFiles() int
	Read() (Block[T], error)
	Seek(blockNum uint64) error
	BlockNum() uint64
	Close() error
}

type reader[T any] struct {
	options        Options
	path           string
	fs             storage.FS
	useCompression bool

	closer io.Closer

	walFiles       []walFile
	currentWALFile int

	lastBlockNum uint64

	decoder Decoder

	mu sync.Mutex
}

func NewReader[T any](opt Options) (Reader[T], error) {
	// apply default options on uninitialized fields
	opt = opt.WithDefaults()

	if opt.Name == "" {
		return nil, fmt.Errorf("wal name cannot be empty")
	}

	var (
		fs  storage.FS
		err error
	)

	walPath := buildETHWALPath(opt.Name, opt.Path)
	if len(walPath) > 0 && walPath[len(walPath)-1] != os.PathSeparator {
		walPath = walPath + string(os.PathSeparator)
	}

	if opt.GoogleCloudStorageBucket != "" {
		fs = storage.NewCloudStorageFS(opt.GoogleCloudStorageBucket, nil)
		fs = gcloud.NewGoogleCloudChecksumStorage(fs)
		fs = storage.NewPrefixWrapper(fs, walPath)

		if opt.CachePath != "" {
			if _, err = os.Stat(opt.CachePath); os.IsNotExist(err) {
				err := os.MkdirAll(opt.CachePath, 0755)
				if err != nil {
					return nil, fmt.Errorf("failed to create WAL directory")
				}
			}
			fs = storage.NewCacheWrapper(fs, storage.NewLocalFS(opt.CachePath), nil)
		}
	} else {
		if _, err = os.Stat(walPath); os.IsNotExist(err) {
			err := os.MkdirAll(walPath, 0755)
			if err != nil {
				return nil, fmt.Errorf("failed to create WAL directory")
			}
		}

		fs = storage.NewLocalFS(walPath)
	}

	walFiles, err := listWALFiles(fs)
	if err != nil {
		return nil, fmt.Errorf("failed to load WAL file list: %w", err)
	}

	return &reader[T]{
		options:  opt,
		path:     walPath,
		fs:       fs,
		walFiles: walFiles,
	}, nil
}

func (r *reader[T]) NumWALFiles() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.walFiles)
}

func (r *reader[T]) Read() (Block[T], error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var err error
	if r.decoder == nil {
		err = r.readFile(firstFileIndex)
		if errors.Is(err, io.EOF) {
			return Block[T]{}, io.EOF
		}
		if err != nil {
			return Block[T]{}, fmt.Errorf("failed to read first WAL file: %w", err)
		}
	}

	var block Block[T]
	for structs.IsZero(block) || block.Number <= r.lastBlockNum {
		err = r.decoder.Decode(&block)
		if err != nil {
			if err == io.EOF {
				err = r.readNextFile()
				if errors.Is(err, io.EOF) {
					return Block[T]{}, io.EOF
				}
				if err != nil {
					return Block[T]{}, fmt.Errorf("failed to read next WAL file: %w", err)
				}

				err = r.decoder.Decode(&block)
				if err != nil {
					return Block[T]{}, fmt.Errorf("failed to decode WAL data: %w", err)
				}

				if !structs.IsZero(block) {
					r.lastBlockNum = block.Number
				}

				if !r.isBlockWithin(block) {
					return Block[T]{}, fmt.Errorf("block number %d is out of wal file %d-%d range",
						block.Number,
						r.walFiles[r.currentWALFile].FirstBlockNum,
						r.walFiles[r.currentWALFile].LastBlockNum)
				}

				return block, nil
			}
			return Block[T]{}, fmt.Errorf("failed to decode WAL data: %w", err)
		}

		if !r.isBlockWithin(block) {
			return Block[T]{}, fmt.Errorf("block number %d is out of wal file %d-%d range",
				block.Number,
				r.walFiles[r.currentWALFile].FirstBlockNum,
				r.walFiles[r.currentWALFile].LastBlockNum)
		}
	}

	if !structs.IsZero(block) {
		r.lastBlockNum = block.Number
	}

	return block, nil
}

func (r *reader[T]) isBlockWithin(block Block[T]) bool {
	return r.walFiles[r.currentWALFile].FirstBlockNum <= block.Number &&
		block.Number <= r.walFiles[r.currentWALFile].LastBlockNum
}

func (r *reader[T]) Seek(blockNum uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	err := r.selectFileForRead(blockNum)
	if err != nil {
		return err
	}

	r.lastBlockNum = blockNum - 1
	return nil
}

func (r *reader[T]) BlockNum() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastBlockNum
}

func (r *reader[T]) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closer != nil {
		return r.closer.Close()
	}
	return nil
}

func (r *reader[T]) readFile(index int) error {
	if index >= len(r.walFiles) {
		return io.EOF
	}

	if r.closer != nil {
		_ = r.closer.Close()
	}

	wFile := r.walFiles[index]
	file, err := r.fs.Open(context.Background(), wFile.Name, &storage.ReaderOptions{})
	if err != nil {
		return err
	}

	walReader := io.NopCloser(file)
	if r.options.NewDecompressor != nil {
		walReader = r.options.NewDecompressor(walReader)
	}

	r.closer = &funcCloser{
		CloseFunc: func() error {
			if err := walReader.Close(); err != nil {
				_ = file.Close()
				return err
			}
			return file.Close()
		},
	}

	r.decoder = r.options.NewDecoder(walReader)

	r.currentWALFile = index
	return nil
}

func (r *reader[T]) readNextFile() error {
	return r.readFile(r.currentWALFile + 1)
}

func (r *reader[T]) selectFileForRead(fromBlockNum uint64) error {
	if len(r.walFiles) > r.currentWALFile && fromBlockNum > r.walFiles[r.currentWALFile].LastBlockNum {
		walFilesLen := len(r.walFiles)
		index := sort.Search(walFilesLen, func(i int) bool {
			return fromBlockNum <= r.walFiles[i].LastBlockNum
		})

		if index == walFilesLen {
			return io.EOF
		}

		if r.currentWALFile != index {
			return r.readFile(index)
		}
	}

	return nil
}
