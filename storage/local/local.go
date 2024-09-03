package local

import (
	"os"

	"github.com/Shopify/go-storage"
)

type LocalFS struct {
	storage.FS
}

func NewLocalFS(path string) *LocalFS {
	if len(path) > 0 && path[len(path)-1] != os.PathSeparator {
		path = path + string(os.PathSeparator)
	}
	return &LocalFS{FS: storage.NewLocalFS(path)}
}
