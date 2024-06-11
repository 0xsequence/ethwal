package ethlogwal

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sync"

	"ethwal/storage"
	"ethwal/storage/local"

	"github.com/c2h5oh/datasize"
)

const defaultBufferSize = 8 * datasize.MB

type Writer[T any] interface {
	Write(b Block[T]) error
	BlockNum() uint64
	Close() error
}

type writer[T any] struct {
	options Options

	path string
	fs   storage.FS

	buffer       *bytes.Buffer
	bufferCloser io.Closer

	firstBlockNum uint64
	lastBlockNum  uint64

	encoder Encoder

	mu sync.Mutex
}

func NewWriter[T any](opt Options) (Writer[T], error) {
	// apply default options on uninitialized fields
	opt = opt.WithDefaults()

	if opt.Dataset.Name == "" {
		return nil, fmt.Errorf("wal name cannot be empty")
	}

	if opt.Dataset.Path == "" {
		return nil, fmt.Errorf("wal path cannot be empty")
	}

	// build WAL path
	walPath := buildETHWALPath(opt.Dataset.Name, opt.Dataset.Version, opt.Dataset.Path)
	if len(walPath) > 0 && walPath[len(walPath)-1] != os.PathSeparator {
		walPath = walPath + string(os.PathSeparator)
	}
	if len(walPath) > 0 && walPath[len(walPath)-1] != os.PathSeparator {
		walPath = walPath + string(os.PathSeparator)
	}

	// create WAL directory if it doesn't exist on local FS
	if _, ok := opt.FileSystem.(*local.LocalFS); ok {
		if _, err := os.Stat(walPath); os.IsNotExist(err) {
			err := os.MkdirAll(walPath, 0755)
			if err != nil {
				return nil, fmt.Errorf("failed to create WAL directory")
			}
		}
	}

	// mount FS with WAL path prefix
	fs := storage.NewPrefixWrapper(opt.FileSystem, walPath)

	walFiles, err := listWALFiles(fs)
	if err != nil {
		return nil, fmt.Errorf("failed to load WAL file list: %w", err)
	}

	var lastBlockNum uint64
	if len(walFiles) > 0 {
		lastBlockNum = walFiles[len(walFiles)-1].LastBlockNum
	}

	return &writer[T]{
		options:       opt,
		path:          walPath,
		fs:            fs,
		firstBlockNum: lastBlockNum + 1,
		lastBlockNum:  lastBlockNum,
		buffer:        bytes.NewBuffer(make([]byte, 0, defaultBufferSize)),
	}, nil
}

func (w *writer[T]) Write(b Block[T]) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.lastBlockNum >= b.Number {
		return nil
	}

	if !w.isReadyToWrite() || w.options.FileRollPolicy.ShouldRoll() {
		if err := w.rollFile(); err != nil {
			return fmt.Errorf("failed to roll to the next WAL file: %w", err)
		}
	}

	err := w.encoder.Encode(b)
	if err != nil {
		return fmt.Errorf("failed to encode WAL data: %w", err)
	}

	w.lastBlockNum = b.Number
	if p, ok := w.options.FileRollPolicy.(LastBlockNumberRollPolicy); ok {
		p.LastBlockNum(w.lastBlockNum)
	}

	return nil
}

func (w *writer[T]) BlockNum() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastBlockNum
}

func (w *writer[T]) Close() error {
	return nil
}

func (w *writer[T]) isReadyToWrite() bool {
	return w.encoder != nil
}

func (w *writer[T]) rollFile() error {
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

		err = w.writeFile()
		if err != nil {
			return err
		}
	}

	return w.newFile()
}

func (w *writer[T]) writeFile() error {
	f, err := w.fs.Create(
		context.Background(),
		fmt.Sprintf("%d_%d.wal", w.firstBlockNum, w.lastBlockNum),
		nil,
	)
	if err != nil {
		return err
	}

	_, err = f.Write(w.buffer.Bytes())
	if err != nil {
		return err
	}

	err = f.Close()
	if err != nil {
		return err
	}
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
	if policy, ok := w.options.FileRollPolicy.(FileSizeRollPolicy); ok {
		bufferWriter = policy.WrapWriter(bufferWriter)
	}

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
