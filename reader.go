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
	FilesNum() int
	Read(ctx context.Context) (Block[T], error)
	Seek(ctx context.Context, blockNum uint64) error
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
		return nil, fmt.Errorf("path cannot be empty")
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
				return nil, fmt.Errorf("failed to create ethwal directory")
			}
		}
	} else {
		// add cache wrapper to file system, so that we can cache the files locally
		if opt.Dataset.CachePath != "" {
			if _, err := os.Stat(opt.Dataset.CachePath); os.IsNotExist(err) {
				err := os.MkdirAll(opt.Dataset.CachePath, 0755)
				if err != nil {
					return nil, fmt.Errorf("failed to create ethwal cache directory")
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

func (r *reader[T]) FilesNum() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.fileIndex.Files())
}

func (r *reader[T]) Read(ctx context.Context) (Block[T], error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var err error
	if r.decoder == nil {
		err = r.readFile(ctx, firstFileIndex)
		if errors.Is(err, io.EOF) {
			return Block[T]{}, io.EOF
		}
		if err != nil {
			return Block[T]{}, fmt.Errorf("failed to read first file: %w", err)
		}
	}

	var block Block[T]
	for structs.IsZero(block) || block.Number <= r.lastBlockNum {
		select {
		case <-ctx.Done():
			return Block[T]{}, ctx.Err()
		default:
		}

		err = r.decoder.Decode(&block)
		if err != nil {
			if err == io.EOF {
				err = r.readNextFile(ctx)
				if errors.Is(err, io.EOF) {
					return Block[T]{}, io.EOF
				}
				if err != nil {
					return Block[T]{}, fmt.Errorf("failed to read next file: %w", err)
				}

				err = r.decoder.Decode(&block)
				if err != nil {
					return Block[T]{}, fmt.Errorf("failed to decode data: %w", err)
				}

				if !structs.IsZero(block) {
					r.lastBlockNum = block.Number
				}

				if !r.isBlockWithin(block) {
					return Block[T]{}, fmt.Errorf("block number %d is out of file block %d-%d range",
						block.Number,
						r.fileIndex.At(r.currFileIndex).FirstBlockNum,
						r.fileIndex.At(r.currFileIndex).LastBlockNum)
				}

				return block, nil
			}
			return Block[T]{}, fmt.Errorf("failed to decode file data: %w", err)
		}

		if !r.isBlockWithin(block) {
			return Block[T]{}, fmt.Errorf("block number %d is out of file block %d-%d range",
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

func (r *reader[T]) Seek(ctx context.Context, blockNum uint64) error {
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
		err = r.readFile(ctx, r.currFileIndex)
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

func (r *reader[T]) readFile(ctx context.Context, index int) error {
	if index >= len(r.fileIndex.Files()) {
		return io.EOF
	}

	if r.closer != nil {
		_ = r.closer.Close()
	}

	file := r.fileIndex.At(index)
	rdr, err := file.Open(ctx, r.fs)
	if err != nil {
		return err
	}

	var decmprRdr = io.NopCloser(rdr)
	if r.options.NewDecompressor != nil {
		decmprRdr = r.options.NewDecompressor(decmprRdr)
	}

	r.closer = &funcCloser{
		CloseFunc: func() error {
			if err := decmprRdr.Close(); err != nil {
				_ = rdr.Close()
				return err
			}
			return rdr.Close()
		},
	}

	r.decoder = r.options.NewDecoder(decmprRdr)

	r.currFileIndex = index
	return nil
}

func (r *reader[T]) readNextFile(ctx context.Context) error {
	return r.readFile(ctx, r.currFileIndex+1)
}
