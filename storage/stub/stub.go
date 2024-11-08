package stub

import (
	"context"
	"fmt"
	"io"

	"github.com/Shopify/go-storage"
)

type Stub struct {
}

func (s Stub) Walk(ctx context.Context, path string, fn storage.WalkFn) error {
	return nil
}

func (s Stub) Open(ctx context.Context, path string, options *storage.ReaderOptions) (*storage.File, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s Stub) Attributes(ctx context.Context, path string, options *storage.ReaderOptions) (*storage.Attributes, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s Stub) Create(ctx context.Context, path string, options *storage.WriterOptions) (io.WriteCloser, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s Stub) Delete(ctx context.Context, path string) error {
	return nil
}

func (s Stub) URL(ctx context.Context, path string, options *storage.SignedURLOptions) (string, error) {
	return "", nil
}

var _ storage.FS = (*Stub)(nil)
