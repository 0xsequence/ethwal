package gcloud

import (
	"bytes"
	"context"
	"errors"
	"hash/crc32"
	"io"

	"github.com/Shopify/go-storage"

	gstorage "cloud.google.com/go/storage"
)

type GoogleCloudChecksumStorage struct {
	storage.FS
}

func NewGoogleCloudChecksumStorage(fs storage.FS) *GoogleCloudChecksumStorage {
	return &GoogleCloudChecksumStorage{FS: fs}
}

func (s *GoogleCloudChecksumStorage) Create(ctx context.Context, name string, opts *storage.WriterOptions) (io.WriteCloser, error) {
	writer, err := s.FS.Create(ctx, name, opts)
	if err != nil {
		return nil, err
	}
	writer_, ok := writer.(*gstorage.Writer)
	if !ok {
		return nil, errors.New("ethlogwal: provided file system does not implement google cloud storage writer")
	}
	return &GoogleCloudChecksumWriter{writer: writer_, buffer: bytes.NewBuffer(nil)}, nil
}

func (s *GoogleCloudChecksumStorage) Open(ctx context.Context, path string, options *storage.ReaderOptions) (*storage.File, error) {
	file, err := s.FS.Open(ctx, path, &storage.ReaderOptions{
		ReadCompressed: false, // This will check crc32 checksum.
	})
	if err != nil {
		return nil, err
	}
	return file, nil
}

type GoogleCloudChecksumWriter struct {
	writer *gstorage.Writer
	buffer *bytes.Buffer
}

func (c *GoogleCloudChecksumWriter) Write(buf []byte) (int, error) {
	return c.buffer.Write(buf)
}

func (c *GoogleCloudChecksumWriter) Close() error {
	c.writer.CRC32C = crc32.Checksum(c.buffer.Bytes(), crc32.MakeTable(crc32.Castagnoli))
	c.writer.SendCRC32C = true
	_, err := c.writer.Write(c.buffer.Bytes())
	if err != nil {
		return err
	}
	return c.writer.Close()
}
