package ethwal

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/0xsequence/ethwal/storage"
	"github.com/0xsequence/ethwal/storage/local"
)

type Dataset struct {
	Name      string
	Version   string
	Path      string
	CachePath string
}

func (d Dataset) FullPath() string {
	return buildETHWALPath(d.Name, d.Version, d.Path)
}

func (d Dataset) FullCachePath() string {
	return buildETHWALPath(d.Name, d.Version, d.CachePath)
}

type Options struct {
	Dataset Dataset

	FileSystem storage.FS

	NewCompressor   NewCompressorFunc
	NewDecompressor NewDecompressorFunc

	NewEncoder NewEncoderFunc
	NewDecoder NewDecoderFunc

	FileRollPolicy  FileRollPolicy
	FileRollOnClose bool
}

func (o Options) WithDefaults() Options {
	if o.FileSystem == nil {
		wd, _ := os.Getwd()
		o.FileSystem = local.NewLocalFS(wd)
	}
	if o.NewEncoder == nil {
		o.NewEncoder = NewCBOREncoder
	}
	if o.NewDecoder == nil {
		o.NewDecoder = NewCBORDecoder
	}
	if o.FileRollPolicy == nil {
		o.FileRollPolicy = NewFileSizeRollPolicy(uint64(defaultBufferSize))
	}
	return o
}

// funcCloser is a helper struct that implements io.Closer interface
type funcCloser struct {
	CloseFunc func() error
}

func (f *funcCloser) Close() error {
	if f.CloseFunc != nil {
		return f.CloseFunc()
	}
	return nil
}

// buildETHWALPath returns the path to the WAL directory
// The path is built as follows: <walPath>/<name?>/<version?>
func buildETHWALPath(name, version, walPath string) string {
	var parts = []string{walPath}

	if name != "" {
		parts = append(parts, name)
	}

	if version != "" {
		parts = append(parts, version)
	}

	return path.Join(walPath, name, version)
}

// ParseWALFileBlockRange reads first and last block number stored in WAL file from file name
func ParseWALFileBlockRange(filePath string) (uint64, uint64) {
	_, fileName := path.Split(filePath)
	fileNameSplit := strings.Split(fileName, ".")
	blockNumberSplit := strings.Split(fileNameSplit[0], "_")

	first, _ := strconv.ParseInt(blockNumberSplit[0], 10, 64)
	last, _ := strconv.ParseInt(blockNumberSplit[1], 10, 64)

	return uint64(first), uint64(last)
}

const FileIndexFileName = ".fileIndex"

var (
	ErrFileNotFound = fmt.Errorf("file not found")
)

type File struct {
	FirstBlockNum uint64 `json:"firstBlockNum" cbor:"0,keyasint"`
	LastBlockNum  uint64 `json:"lastBlockNum" cbor:"1,keyasint"`
}

func (f File) Path() string {
	hash := sha256.Sum256([]byte(f.LegacyPath()))
	return fmt.Sprintf("%x/%x/%x/%x", hash[0:8], hash[8:16], hash[16:24], hash[24:32])
}

func (f File) LegacyPath() string {
	return fmt.Sprintf("%d_%d.wal", f.FirstBlockNum, f.LastBlockNum)
}

func (f File) Create(ctx context.Context, fs storage.FS) (io.WriteCloser, error) {
	filePath := f.Path()
	if _, ok := fs.(*local.LocalFS); ok {
		dirPath := path.Dir(filePath)
		if _, err := os.Stat(dirPath); os.IsNotExist(err) {
			err := os.MkdirAll(dirPath, 0755)
			if err != nil {
				return nil, fmt.Errorf("failed to create file directory")
			}
		}
	}
	return fs.Create(ctx, filePath, nil)
}

func (f File) Open(ctx context.Context, fs storage.FS) (io.ReadCloser, error) {
	if f.exist(ctx, fs) {
		return fs.Open(ctx, f.Path(), nil)
	}
	return fs.Open(ctx, f.LegacyPath(), nil)
}

func (f File) Exist(ctx context.Context, fs storage.FS) bool {
	return f.exist(ctx, fs) || f.existLegacy(ctx, fs)
}

func (f File) exist(ctx context.Context, fs storage.FS) bool {
	_, err := fs.Attributes(ctx, f.Path(), nil)
	if err != nil {
		return false
	}
	return true
}

func (f File) existLegacy(ctx context.Context, fs storage.FS) bool {
	_, err := fs.Attributes(ctx, f.LegacyPath(), nil)
	if err != nil {
		return false
	}
	return true
}

type FileIndex struct {
	fs storage.FS

	files []File
}

func NewFileIndex(fs storage.FS) (*FileIndex, error) {
	files, err := ListFiles(context.Background(), fs)
	if err != nil {
		return nil, err
	}

	return &FileIndex{
		fs:    fs,
		files: files,
	}, nil
}

func NewFileIndexFromFiles(fs storage.FS, files []File) *FileIndex {
	sort.Slice(files, func(i, j int) bool {
		return files[i].FirstBlockNum < files[j].FirstBlockNum
	})

	return &FileIndex{
		fs:    fs,
		files: files,
	}
}

func (fi *FileIndex) Files() []File {
	return fi.files
}

func (fi *FileIndex) AddFile(file File) error {
	_, _, err := fi.FindFile(file.LastBlockNum)
	if err == nil {
		return fmt.Errorf("ethlogwal: file for block %d already exists", file.LastBlockNum)
	}

	fi.files = append(fi.files, file)
	return nil
}

func (fi *FileIndex) At(index int) File {
	return fi.files[index]
}

func (fi *FileIndex) FindFile(blockNum uint64) (File, int, error) {
	i := sort.Search(len(fi.files), func(i int) bool {
		return blockNum <= fi.files[i].LastBlockNum
	})
	if i == len(fi.files) {
		return File{}, 0, ErrFileNotFound
	}
	return fi.files[i], i, nil
}

func (fi *FileIndex) Save(ctx context.Context) error {
	indexFile, err := fi.fs.Create(ctx, FileIndexFileName, nil)
	if err != nil {
		return err
	}

	comp := NewZSTDCompressor(indexFile)
	enc := NewCBOREncoder(comp)
	for _, file := range fi.files {
		_ = enc.Encode(file)
	}

	err = comp.Close()
	if err != nil {
		return err
	}
	return indexFile.Close()
}

// ListFiles lists all WAL files in the provided file system
func ListFiles(ctx context.Context, fs storage.FS) ([]File, error) {
	indexFile, err := fs.Open(context.Background(), FileIndexFileName, nil)
	if err != nil && strings.Contains(err.Error(), "not exist") {
		err = migrateToFileIndex(ctx, fs)
		if err != nil {
			return nil, err
		}

		indexFile, err = fs.Open(context.Background(), FileIndexFileName, nil)
		if err != nil {
			return nil, err
		}
	}
	if err != nil {
		return nil, err
	}
	defer indexFile.Close()

	var files []File
	decomp := NewZSTDDecompressor(indexFile)
	dec := NewCBORDecoder(decomp)

	for {
		var file File
		err = dec.Decode(&file)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		files = append(files, file)
	}

	if len(files) != 0 && !files[len(files)-1].Exist(ctx, fs) {
		files = files[:len(files)-1]
	}

	return files, nil
}

func migrateToFileIndex(ctx context.Context, fs storage.FS) error {
	wlk, ok := fs.(storage.Walker)
	if !ok {
		return fmt.Errorf("ethlogwal: provided file system does not implement Walker interface")
	}

	var files []File
	err := wlk.Walk(ctx, "", func(filePath string) error {
		// walk only wal files
		if path.Ext(filePath) != ".wal" {
			return nil
		}

		_, fileName := path.Split(filePath)
		firstBlockNum, lastBlockNum := ParseWALFileBlockRange(fileName)
		files = append(files, File{
			FirstBlockNum: firstBlockNum,
			LastBlockNum:  lastBlockNum,
		})
		return nil
	})
	if err != nil {
		return err
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].FirstBlockNum < files[j].FirstBlockNum
	})

	fileIndex := NewFileIndexFromFiles(fs, files)
	return fileIndex.Save(ctx)
}
