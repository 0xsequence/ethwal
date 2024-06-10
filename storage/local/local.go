package local

import "github.com/Shopify/go-storage"

type LocalFS struct {
	storage.FS
}

func NewLocalFS(path string) *LocalFS {
	return &LocalFS{FS: storage.NewLocalFS(path)}
}
