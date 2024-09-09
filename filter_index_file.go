package ethwal

import (
	"context"
	"fmt"

	"github.com/0xsequence/ethwal/storage"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

type indexFile struct {
	fs   storage.FS
	path string
}

func newIndexFile(fs storage.FS, indexName IndexName, key string) (*indexFile, error) {
	path := indexPath(string(indexName), key)
	return &indexFile{fs: fs, path: path}, nil
}

func (i *indexFile) Read(ctx context.Context) (*roaring64.Bitmap, error) {
	file, err := i.fs.Open(ctx, i.path, nil)
	if err != nil {
		// TODO: decide if we should report an error or just create a new roaring bitmap...
		// with this approach we are not reporting an error if the file does not exist
		// and we just write the new bitmap when write is called...
		// return nil, fmt.Errorf("failed to open index file: %w", err)
		return roaring64.New(), nil
	}
	defer file.Close()
	var buf []byte = make([]byte, file.Size)
	_, err = file.Read(buf)
	if err != nil {
		return nil, fmt.Errorf("failed to read index file: %w", err)
	}
	bmap := roaring64.New()
	err = bmap.UnmarshalBinary(buf)

	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal bitmap: %w", err)
	}
	return bmap, nil
}

func (i *indexFile) Write(ctx context.Context, bmap *roaring64.Bitmap) error {
	file, err := i.fs.Create(ctx, i.path, nil)
	if err != nil {
		return fmt.Errorf("failed to open index file: %w", err)
	}
	defer file.Close()
	data, err := bmap.ToBytes()
	if err != nil {
		return fmt.Errorf("failed to marshal bitmap: %w", err)
	}
	_, err = file.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write bitmap: %w", err)
	}
	return nil
}
