package ethwal

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/0xsequence/ethwal/storage"
	"github.com/0xsequence/ethwal/storage/local"
)

type Writer[T any] interface {
	FileSystem() storage.FS
	Write(ctx context.Context, b Block[T]) error
	BlockNum() uint64
	RollFile(ctx context.Context) error
	Close(ctx context.Context) error
	Options() Options
	SetOptions(opt Options)
}

type writer[T any] struct {
	options Options

	path string
	fs   storage.FS

	buffer       *bytes.Buffer
	bufferCloser io.Closer

	firstBlockNum uint64
	lastBlockNum  uint64

	fileIndex *FileIndex

	encoder Encoder

	mu sync.Mutex
}

func NewWriter[T any](opt Options) (Writer[T], error) {
	// apply default options on uninitialized fields
	opt = opt.WithDefaults()

	if opt.Dataset.Path == "" {
		return nil, fmt.Errorf("path cannot be empty")
	}

	// build dataset path
	datasetPath := opt.Dataset.FullPath()

	// create dataset directory if it doesn't exist on local FS
	if _, ok := opt.FileSystem.(*local.LocalFS); ok {
		if _, err := os.Stat(datasetPath); os.IsNotExist(err) {
			err := os.MkdirAll(datasetPath, 0755)
			if err != nil {
				return nil, fmt.Errorf("failed to create ethwal directory")
			}
		}
	}

	// mount FS with dataset path prefix
	fs := storage.NewPrefixWrapper(opt.FileSystem, datasetPath)

	// create file IndexBlock
	fileIndex := NewFileIndex(fs)

	// load file IndexBlock
	ctx, cancel := context.WithTimeout(context.Background(), loadIndexFileTimeout)
	defer cancel()

	err := fileIndex.Load(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load file IndexBlock: %w", err)
	}

	var lastBlockNum uint64
	var fileIndexFileList = fileIndex.Files()
	if len(fileIndexFileList) > 0 {
		lastBlockNum = fileIndexFileList[len(fileIndexFileList)-1].LastBlockNum
	}

	// create new writer
	return &writer[T]{
		options:       opt,
		path:          datasetPath,
		fs:            fs,
		firstBlockNum: lastBlockNum + 1,
		lastBlockNum:  lastBlockNum,
		fileIndex:     fileIndex,
		buffer:        bytes.NewBuffer(make([]byte, 0, defaultFileSize)),
	}, nil
}

func (w *writer[T]) FileSystem() storage.FS {
	return w.fs
}

func (w *writer[T]) Write(ctx context.Context, b Block[T]) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.lastBlockNum >= b.Number {
		return nil
	}

	if !w.isReadyToWrite() || w.options.FileRollPolicy.ShouldRoll() {
		if err := w.rollFile(ctx); err != nil {
			return fmt.Errorf("failed to roll to the next file: %w", err)
		}
	}

	err := w.encoder.Encode(b)
	if err != nil {
		return fmt.Errorf("failed to encode file data: %w", err)
	}

	w.lastBlockNum = b.Number
	w.options.FileRollPolicy.onBlockProcessed(w.lastBlockNum)
	return nil
}

func (w *writer[T]) RollFile(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.rollFile(ctx)
}

func (w *writer[T]) BlockNum() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastBlockNum
}

func (w *writer[T]) Close(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.options.FileRollOnClose {
		// close previous buffer and write file to fs
		if w.bufferCloser != nil {
			// skip if there are no blocks to write
			if w.lastBlockNum < w.firstBlockNum {
				return nil
			}

			err := w.bufferCloser.Close()
			if err != nil {
				return err
			}

			err = w.writeFile(ctx)
			if err != nil {
				return err
			}
		}
		w.bufferCloser = nil
	}
	return nil
}

func (w *writer[T]) Options() Options {
	return w.options
}

func (w *writer[T]) SetOptions(opt Options) {
	w.options = opt
}

func (w *writer[T]) isReadyToWrite() bool {
	return w.encoder != nil
}

func (w *writer[T]) rollFile(ctx context.Context) error {
	// close previous buffer and write file to fs
	if w.bufferCloser != nil {
		// skip if there are no blocks to write
		if w.lastBlockNum < w.firstBlockNum {
			return nil
		}

		err := w.bufferCloser.Close()
		if err != nil {
			return err
		}

		err = w.writeFile(ctx)
		if err != nil {
			return err
		}
	}

	return w.newFile()
}

func (w *writer[T]) writeFile(ctx context.Context) error {
	// create new file
	newFile := &File{FirstBlockNum: w.firstBlockNum, LastBlockNum: w.lastBlockNum}
	w.options.FileRollPolicy.onFlush(ctx)

	// add file to file IndexBlock
	err := w.fileIndex.AddFile(newFile)
	if err != nil {
		return err
	}

	// save file IndexBlock
	err = w.fileIndex.Save(ctx)
	if err != nil {
		return err
	}

	// save file
	f, err := newFile.Create(ctx, w.fs)
	if err != nil {
		return err
	}

	_, err = f.Write(w.buffer.Bytes())
	if err != nil {
		_ = f.Close()
		return err
	}

	err = f.Close()
	if err != nil {
		return err
	}

	// wait for both file and file IndexBlock to be saved
	// todo: save in background
	return nil
}

func (w *writer[T]) newFile() error {
	// update block numbers
	w.firstBlockNum = w.lastBlockNum + 1

	// reset buffer
	w.buffer.Reset()

	// reset file roll policy
	w.options.FileRollPolicy.Reset()

	// create new buffer writer
	bufferWriter := io.Writer(w.buffer)
	bufferWriter = &writerWrapper{Writer: bufferWriter, fsrp: w.options.FileRollPolicy}

	// create new buffer closer
	w.bufferCloser = &funcCloser{
		CloseFunc: func() error {
			return nil
		},
	}

	// wrap buffer writer with compression writer
	if w.options.NewCompressor != nil {
		zw := w.options.NewCompressor(bufferWriter)
		bufferWriter = zw
		w.bufferCloser = zw
	}

	// create new encoder
	w.encoder = w.options.NewEncoder(bufferWriter)
	return nil
}
