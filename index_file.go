package ethwal

import (
	"context"
	"fmt"

	"github.com/0xsequence/ethwal/storage"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

type IndexFile struct {
	fs   storage.FS
	path string
}

func NewIndexFile(fs storage.FS, indexName IndexName, key string) (*IndexFile, error) {
	path := indexPath(string(indexName), key)
	return &IndexFile{fs: fs, path: path}, nil
}

func (i *IndexFile) Read(ctx context.Context) (*roaring64.Bitmap, error) {
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

func (i *IndexFile) Write(ctx context.Context, bmap *roaring64.Bitmap) error {
	file, err := i.fs.Create(ctx, i.path, nil)
	if err != nil {
		return fmt.Errorf("failed to open index file: %w", err)
	}
	defer file.Close()
	_, err = bmap.WriteTo(file)
	return err
}
