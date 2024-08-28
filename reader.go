package ethwal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/fatih/structs"

	"github.com/0xsequence/ethwal/storage"
	"github.com/0xsequence/ethwal/storage/local"
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

	fileIndex     *FileIndex
	currFileIndex int

	lastBlockNum uint64

	decoder Decoder

	mu sync.Mutex
}

func NewReader[T any](opt Options) (Reader[T], error) {
	// apply default options on uninitialized fields
	opt = opt.WithDefaults()

	if opt.Dataset.Path == "" {
		return nil, fmt.Errorf("wal path cannot be empty")
	}

	// build WAL path
	walPath := buildETHWALPath(opt.Dataset.Name, opt.Dataset.Version, opt.Dataset.Path)
	if len(walPath) > 0 && walPath[len(walPath)-1] != os.PathSeparator {
		walPath = walPath + string(os.PathSeparator)
	}

	// set file system
	fs := opt.FileSystem

	// create WAL directory if it doesn't exist on local FS
	if _, ok := opt.FileSystem.(*local.LocalFS); ok {
		if _, err := os.Stat(walPath); os.IsNotExist(err) {
			err := os.MkdirAll(walPath, 0755)
			if err != nil {
				return nil, fmt.Errorf("failed to create WAL directory")
			}
		}
	} else {
		// add cache wrapper to file system, so that we can cache the files locally
		if opt.Dataset.CachePath != "" {
			if _, err := os.Stat(opt.Dataset.CachePath); os.IsNotExist(err) {
				err := os.MkdirAll(opt.Dataset.CachePath, 0755)
				if err != nil {
					return nil, fmt.Errorf("failed to create WAL directory")
				}
			}
			fs = storage.NewCacheWrapper(fs, local.NewLocalFS(opt.Dataset.CachePath), nil)
		}
	}

	// add prefix to file system
	fs = storage.NewPrefixWrapper(fs, walPath)

	fileIndex, err := NewFileIndex(fs)
	if err != nil {
		return nil, fmt.Errorf("failed to load file index: %w", err)
	}

	return &reader[T]{
		options:   opt,
		path:      walPath,
		fs:        fs,
		fileIndex: fileIndex,
	}, nil
}

func (r *reader[T]) NumWALFiles() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.fileIndex.Files())
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
						r.fileIndex.At(r.currFileIndex).FirstBlockNum,
						r.fileIndex.At(r.currFileIndex).LastBlockNum)
				}

				return block, nil
			}
			return Block[T]{}, fmt.Errorf("failed to decode WAL data: %w", err)
		}

		if !r.isBlockWithin(block) {
			return Block[T]{}, fmt.Errorf("block number %d is out of wal file %d-%d range",
				block.Number,
				r.fileIndex.At(r.currFileIndex).FirstBlockNum,
				r.fileIndex.At(r.currFileIndex).LastBlockNum)
		}
	}

	if !structs.IsZero(block) {
		r.lastBlockNum = block.Number
	}

	return block, nil
}

func (r *reader[T]) isBlockWithin(block Block[T]) bool {
	return r.fileIndex.Files()[r.currFileIndex].FirstBlockNum <= block.Number &&
		block.Number <= r.fileIndex.Files()[r.currFileIndex].LastBlockNum
}

func (r *reader[T]) Seek(blockNum uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	_, fileIndex, err := r.fileIndex.FindFile(blockNum)
	if err != nil && errors.Is(err, ErrFileNotFound) {
		return io.EOF
	}
	if err != nil {
		return err
	}

	if r.currFileIndex != fileIndex {
		r.currFileIndex = fileIndex
		err = r.readFile(fileIndex)
		if err != nil {
			return err
		}
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
	if index >= len(r.fileIndex.Files()) {
		return io.EOF
	}

	if r.closer != nil {
		_ = r.closer.Close()
	}

	wFile := r.fileIndex.At(index)
	file, err := wFile.Open(context.Background(), r.fs)
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

	r.currFileIndex = index
	return nil
}

func (r *reader[T]) readNextFile() error {
	return r.readFile(r.currFileIndex + 1)
}
