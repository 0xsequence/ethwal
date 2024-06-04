package ethlogwal

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"sync"

	"github.com/DataDog/zstd"
	"github.com/Shopify/go-storage"
)

type Writer[T any] interface {
	Write(b Block[T]) error
	BlockNum() uint64
	Close() error
}

type writer[T any] struct {
	options        Options
	path           string
	fs             storage.FS
	useCompression bool
	maxWALSize     uint64

	stats  *fileStats
	closer io.Closer
	buffer *bytes.Buffer

	firstBlockNum uint64
	lastBlockNum  uint64

	encoder Encoder

	mu sync.Mutex
}

func NewWriter[T any](opt Options) (Writer[T], error) {
	if opt.Name == "" {
		return nil, fmt.Errorf("wal name cannot be empty")
	}

	if opt.MaxWALSize == 0 {
		return nil, fmt.Errorf("wal max size cannot be empty")
	}

	var (
		fs  storage.FS
		err error
	)

	opt.Path = path.Join(opt.Path, opt.Name, WALFormatVersion)

	if len(opt.Path) > 0 && opt.Path[len(opt.Path)-1] != os.PathSeparator {
		opt.Path = opt.Path + string(os.PathSeparator)
	}

	if opt.GoogleCloudStorageBucket != "" {
		fs = storage.NewCloudStorageFS(opt.GoogleCloudStorageBucket, nil)
		fs = NewGoogleCloudChecksumStorage(fs, int(opt.MaxWALSize+(opt.MaxWALSize/10)))
		fs = storage.NewPrefixWrapper(fs, opt.Path)
	} else {
		if _, err = os.Stat(opt.Path); os.IsNotExist(err) {
			err := os.MkdirAll(opt.Path, 0755)
			if err != nil {
				return nil, fmt.Errorf("failed to create WAL directory")
			}
		}

		fs = storage.NewLocalFS(opt.Path)
	}

	walFiles, err := listWALFiles(fs)
	if err != nil {
		return nil, fmt.Errorf("failed to load WAL file list: %w", err)
	}

	var lastBlockNum uint64
	if len(walFiles) > 0 {
		lastBlockNum = walFiles[len(walFiles)-1].LastBlockNum
	}

	return &writer[T]{
		options:        opt,
		path:           opt.Path,
		fs:             fs,
		useCompression: opt.UseCompression,
		maxWALSize:     opt.MaxWALSize,
		lastBlockNum:   lastBlockNum,
		buffer:         bytes.NewBuffer(make([]byte, 0, opt.MaxWALSize)),
	}, nil
}

func (w *writer[T]) Write(b Block[T]) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.lastBlockNum >= b.BlockNumber {
		return nil
	}

	if w.isMaxFileSizeReached() {
		if err := w.writeNextFile(); err != nil {
			return fmt.Errorf("failed to write next WAL file: %w", err)
		}
		w.firstBlockNum = b.BlockNumber
	}

	err := w.encoder.Encode(b)
	if err != nil {
		return fmt.Errorf("failed to encode WAL data: %w", err)
	}

	w.lastBlockNum = b.BlockNumber

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

func (w *writer[T]) isMaxFileSizeReached() bool {
	return w.stats == nil || w.stats.BytesWritten > w.maxWALSize
}

func (w *writer[T]) writeNextFile() error {
	if w.closer != nil {
		err := w.closer.Close()
		if err != nil {
			return err
		}

		err = w.writeNextFileToFS()
		if err != nil {
			return err
		}
	}

	w.stats = &fileStats{Writer: w.buffer}
	w.closer = &funcCloser{
		CloseFunc: func() error {
			return nil
		},
	}

	writer := io.Writer(w.stats)
	if w.useCompression {
		zw := zstd.NewWriterLevel(writer, zstd.BestSpeed)

		writer = zw
		w.closer = zw
	}

	if w.options.UseJSONEncoding {
		w.encoder = newJSONEncoder(writer)
	} else {
		w.encoder = newBinaryEncoder(writer)
	}

	return nil
}

func (w *writer[T]) writeNextFileToFS() error {
	f, err := w.fs.Create(
		context.Background(),
		fmt.Sprintf("%d_%d.wal", w.firstBlockNum, w.lastBlockNum),
		&storage.WriterOptions{},
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

	w.buffer.Reset()
	return nil
}
