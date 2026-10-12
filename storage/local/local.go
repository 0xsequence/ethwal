package local

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/Shopify/go-storage"
)

// createFileMode is the mode of created files, as os.Create gives them
// (0666) under the usual 022 umask.
const createFileMode = os.FileMode(0o644)

type LocalFS struct {
	storage.FS

	root string
}

func NewLocalFS(path string) *LocalFS {
	if len(path) > 0 && path[len(path)-1] != os.PathSeparator {
		path = path + string(os.PathSeparator)
	}
	return &LocalFS{FS: storage.NewLocalFS(path), root: path}
}

// Create writes the file atomically: the data goes to a hidden temporary file
// next to path, which Close syncs and renames over path. A reader therefore
// sees either the previous file or the complete new one, never a partial
// write, and a crash mid-write leaves the previous file intact. This matters
// for files rewritten in place, such as the .fileIndex. If any write fails,
// Close removes the temporary file and keeps the previous one.
func (l *LocalFS) Create(_ context.Context, path string, options *storage.WriterOptions) (io.WriteCloser, error) {
	full := filepath.Join(l.root, path)
	dir := filepath.Dir(full)
	if err := os.MkdirAll(dir, storage.LocalCreatePathMode); err != nil {
		return nil, err
	}
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(full)+".tmp-*")
	if err != nil {
		return nil, err
	}
	if err := tmp.Chmod(createFileMode); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		return nil, err
	}
	return &atomicFile{File: tmp, path: full, options: options}, nil
}

// atomicFile is a temporary file published at path by Close.
type atomicFile struct {
	*os.File
	path     string
	options  *storage.WriterOptions
	writeErr error
	closed   bool
}

func (f *atomicFile) Write(p []byte) (int, error) {
	n, err := f.File.Write(p)
	if err != nil && f.writeErr == nil {
		f.writeErr = err
	}
	return n, err
}

func (f *atomicFile) Close() error {
	if f.closed {
		return nil
	}
	f.closed = true
	tmp := f.File.Name()
	if f.writeErr != nil {
		_ = f.File.Close()
		_ = os.Remove(tmp)
		return f.writeErr
	}
	if err := errors.Join(f.File.Sync(), f.File.Close()); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, f.path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if f.options != nil {
		modTime := f.options.Attributes.ModTime
		if !f.options.Attributes.CreationTime.IsZero() {
			// There is no way to store the creation time, so it becomes the
			// modification time, as in go-storage's local FS.
			modTime = f.options.Attributes.CreationTime
		}
		if !modTime.IsZero() {
			return os.Chtimes(f.path, modTime, modTime)
		}
	}
	return nil
}
