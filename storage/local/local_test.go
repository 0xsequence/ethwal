package local

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCreatePublishesOnClose(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	fs := NewLocalFS(dir)
	target := filepath.Join(dir, "a", "b", "file")

	w, err := fs.Create(ctx, "a/b/file", nil)
	require.NoError(t, err)
	_, err = w.Write([]byte("new"))
	require.NoError(t, err)
	_, err = os.Stat(target)
	require.True(t, os.IsNotExist(err), "nothing is visible at the path before Close")

	require.NoError(t, w.Close())
	got, err := os.ReadFile(target)
	require.NoError(t, err)
	require.Equal(t, "new", string(got))
	st, err := os.Stat(target)
	require.NoError(t, err)
	require.Equal(t, createFileMode, st.Mode().Perm())
	require.NoError(t, w.Close(), "a second Close is a no-op")
	requireNoTemp(t, filepath.Dir(target))
}

func TestCreateKeepsPreviousFileUntilClose(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	fs := NewLocalFS(dir)
	target := filepath.Join(dir, ".fileIndex")
	require.NoError(t, os.WriteFile(target, []byte("old content"), 0o644))

	w, err := fs.Create(ctx, ".fileIndex", nil)
	require.NoError(t, err)
	_, err = w.Write([]byte("new"))
	require.NoError(t, err)
	got, err := os.ReadFile(target)
	require.NoError(t, err)
	require.Equal(t, "old content", string(got), "a reader sees the previous file, not a truncated one")

	require.NoError(t, w.Close())
	got, err = os.ReadFile(target)
	require.NoError(t, err)
	require.Equal(t, "new", string(got))
	requireNoTemp(t, dir)
}

func TestCreateDiscardsFailedWrite(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	fs := NewLocalFS(dir)
	target := filepath.Join(dir, "file")
	require.NoError(t, os.WriteFile(target, []byte("old"), 0o644))

	w, err := fs.Create(ctx, "file", nil)
	require.NoError(t, err)
	af := w.(*atomicFile)
	require.NoError(t, af.File.Close()) // make the next write fail
	_, err = w.Write([]byte("partial"))
	require.Error(t, err)
	require.Error(t, w.Close(), "Close reports the write error")

	got, err := os.ReadFile(target)
	require.NoError(t, err)
	require.Equal(t, "old", string(got), "a failed write never replaces the file")
	requireNoTemp(t, dir)
}

func requireNoTemp(t *testing.T, dir string) {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, ".*.tmp-*"))
	require.NoError(t, err)
	require.Empty(t, matches, "temporary files are cleaned up")
}
