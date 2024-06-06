package ethlogwal

import (
	"bytes"
	"context"
	"ethwal/storage/gcloud"
	"fmt"
	"io"
	"os"
	"path"
	"sync"

	"github.com/DataDog/zstd"
	"github.com/Shopify/go-storage"
	"github.com/c2h5oh/datasize"
)

const defaultBufferSize = 8 * datasize.MB

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

	fileRollPolicy FileRollPolicy
	buffer         *bytes.Buffer
	bufferCloser   io.Closer

	firstBlockNum uint64
	lastBlockNum  uint64

	encoder Encoder

	mu sync.Mutex
}

func NewWriter[T any](opt Options) (Writer[T], error) {
	if opt.Name == "" {
		return nil, fmt.Errorf("wal name cannot be empty")
	}

	if opt.FileRollPolicy == nil {
		opt.FileRollPolicy = NewFileSizeRollPolicy(uint64(defaultBufferSize))
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
		fs = gcloud.NewGoogleCloudChecksumStorage(fs)
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
		firstBlockNum:  lastBlockNum + 1,
		lastBlockNum:   lastBlockNum,
		fileRollPolicy: opt.FileRollPolicy,
		buffer:         bytes.NewBuffer(make([]byte, 0, defaultBufferSize)),
	}, nil
}

func (w *writer[T]) Write(b Block[T]) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.lastBlockNum >= b.Number {
		return nil
	}

	if !w.isInitialized() || w.fileRollPolicy.ShouldRoll() {
		if err := w.rollFile(); err != nil {
			return fmt.Errorf("failed to roll to the next WAL file: %w", err)
		}
	}

	err := w.encoder.Encode(b)
	if err != nil {
		return fmt.Errorf("failed to encode WAL data: %w", err)
	}

	w.lastBlockNum = b.Number

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

func (w *writer[T]) isInitialized() bool {
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
	return nil
}

func (w *writer[T]) newFile() error {
	// update block numbers
	w.firstBlockNum = w.lastBlockNum + 1

	// reset buffer
	w.buffer.Reset()

	// create new buffer writer
	bufferWriter := io.Writer(w.buffer)
	if policy, ok := w.fileRollPolicy.(*fileSizeRollPolicy); ok {
		bufferWriter = policy.WrapWriter(bufferWriter)
	}

	// create new buffer closer
	w.bufferCloser = &funcCloser{
		CloseFunc: func() error {
			return nil
		},
	}

	// wrap buffer writer with compression writer
	if w.useCompression {
		zw := zstd.NewWriterLevel(bufferWriter, zstd.BestSpeed)
		bufferWriter = zw
		w.bufferCloser = zw
	}

	// create new encoder
	if w.options.UseJSONEncoding {
		w.encoder = newJSONEncoder(bufferWriter)
	} else {
		w.encoder = newBinaryEncoder(bufferWriter)
	}

	return nil
}
