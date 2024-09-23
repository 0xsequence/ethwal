package ethwal

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/0xsequence/ethwal/storage"
	"github.com/0xsequence/ethwal/storage/local"
	"github.com/c2h5oh/datasize"
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

// buildETHWALPath returns the path to the WAL directory
// The path is built as follows: <walPath>/<name?>/<version?>
func buildETHWALPath(name, version, rootPath string) string {
	var parts = []string{rootPath}

	if name != "" {
		parts = append(parts, name)
	}

	if version != "" {
		parts = append(parts, version)
	}

	retPath := path.Join(parts...)
	if len(retPath) > 0 && retPath[len(retPath)-1] != os.PathSeparator {
		retPath = retPath + string(os.PathSeparator)
	}
	return retPath
}

const (
	defaultFileSize        = 8 * datasize.MB
	defaultPrefetchTimeout = 30 * time.Second
)

type Options struct {
	Dataset Dataset

	FileSystem storage.FS

	NewCompressor   NewCompressorFunc
	NewDecompressor NewDecompressorFunc

	NewEncoder NewEncoderFunc
	NewDecoder NewDecoderFunc

	FileRollPolicy  FileRollPolicy
	FileRollOnClose bool

	FilePrefetchTimeout time.Duration
}

func (o Options) WithDefaults() Options {
	o.FileSystem = cmp.Or(o.FileSystem, storage.FS(local.NewLocalFS("")))
	o.FilePrefetchTimeout = cmp.Or(o.FilePrefetchTimeout, defaultPrefetchTimeout)
	o.FileRollPolicy = cmp.Or(o.FileRollPolicy, NewFileSizeRollPolicy(uint64(defaultFileSize)))
	if o.NewEncoder == nil {
		o.NewEncoder = NewCBOREncoder
	}
	if o.NewDecoder == nil {
		o.NewDecoder = NewCBORDecoder
	}
	return o
}

const FileIndexFileName = ".fileIndex"
const NumberOfDirectoriesPerLevel = 1000 // since there are 3 levels the maximal number of directories is 1000^3 = 1_000_000_000

var (
	ErrFileNotExist = fmt.Errorf("file does not exist")
)

type File struct {
	FirstBlockNum uint64 `json:"firstBlockNum" cbor:"0,keyasint"`
	LastBlockNum  uint64 `json:"lastBlockNum" cbor:"1,keyasint"`

	prefetchBuffer []byte
	prefetchCtx    context.Context

	mu sync.Mutex
}

// Path returns the path to the file
//
// The directory structure:
//
//	-- ethwal
//		|-- 000563
//		|   |-- 000256
//		|   |   |-- 000124
//		|   |   |   |-- 28f55a4df523b3ef28f55a4df523b3ef28f55a4df523b3ef28f55a4df523b3ef <- ethwal file
//		|   |   |-- 000278
//		|   |   |   |-- 28f55a4df523b3ef28f55a4df523b3ef28f55a4df523b3ef28f55a4df523b3dd <- ethwal file
//		|   |-- 000025
//		|   |   |-- 000967
//		|   |   |   |-- 28f55a4df523b3ef28f55a4df523b3ef28f55a4df523b3dd28f55a4df523b3ef <- ethwal file
//		|-- .fileIndex
//
// The data structure ensures that there is no more than 1000 directories per level. The filename is a sha-256 hash of
// the first and last block numbers. The hash is used to distribute files evenly across directories.
func (f *File) Path() string {
	// prepare data for hashing
	var (
		hash [32]byte
		data [16]byte
	)

	binary.BigEndian.PutUint64(data[0:8], f.FirstBlockNum)
	binary.BigEndian.PutUint64(data[8:16], f.LastBlockNum)

	// hash the data
	hash = sha256.Sum256(data[:])

	// return the path, remember to update the format if you change NumberOfDirectoriesPerLevel
	return fmt.Sprintf("%06d/%06d/%06d/%x",
		binary.BigEndian.Uint64(hash[0:8])%NumberOfDirectoriesPerLevel,   // level0
		binary.BigEndian.Uint64(hash[8:16])%NumberOfDirectoriesPerLevel,  // level1
		binary.BigEndian.Uint64(hash[16:24])%NumberOfDirectoriesPerLevel, // level2
		hash, // filename
	)
}

func (f *File) Create(ctx context.Context, fs storage.FS) (io.WriteCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return fs.Create(ctx, f.Path(), nil)
}

func (f *File) Open(ctx context.Context, fs storage.FS) (io.ReadCloser, error) {
	prefetchedRdr := f.prefetched()
	if prefetchedRdr != nil {
		return prefetchedRdr, nil
	}
	return f.open(ctx, fs)
}

func (f *File) Prefetch(ctx context.Context, fs storage.FS) error {
	f.mu.Lock()
	// check if is already prefetched
	if f.prefetchBuffer != nil {
		f.mu.Unlock()
		return nil
	}
	// check if prefetch is in progress
	if f.prefetchCtx != nil {
		prefetchCtx := f.prefetchCtx
		<-prefetchCtx.Done()
		f.mu.Unlock()
		return nil
	}

	// prepare prefetch context
	prefetchCtx, cancelPrefetch := context.WithCancel(ctx)
	defer cancelPrefetch()

	// set prefetch context
	f.prefetchCtx = prefetchCtx
	f.mu.Unlock()

	rdr, err := f.open(ctx, fs)
	if err != nil {
		return err
	}

	buff, err := io.ReadAll(rdr)
	if err != nil {
		_ = rdr.Close()
		return err
	}

	f.mu.Lock()
	f.prefetchBuffer = buff
	f.mu.Unlock()
	return rdr.Close()
}

func (f *File) PrefetchClear() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.prefetchBuffer = nil
}

func (f *File) Exist(ctx context.Context, fs storage.FS) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.exist(ctx, fs) || f.existLegacy(ctx, fs)
}

func (f *File) legacyPath() string {
	return fmt.Sprintf("%d_%d.wal", f.FirstBlockNum, f.LastBlockNum)
}

func (f *File) open(ctx context.Context, fs storage.FS) (io.ReadCloser, error) {
	if f.exist(ctx, fs) {
		return fs.Open(ctx, f.Path(), nil)
	}

	file, err := fs.Open(ctx, f.legacyPath(), nil)
	if err != nil && storage.IsNotExist(err) {
		return nil, ErrFileNotExist
	}
	if err != nil {
		return nil, err
	}
	return file, nil
}

func (f *File) prefetched() io.ReadCloser {
	f.mu.Lock()
	prefetchCtx := f.prefetchCtx
	prefetchBuffer := f.prefetchBuffer
	f.prefetchBuffer = nil
	f.mu.Unlock()

	if prefetchBuffer != nil {
		// already prefetched
		rdr := io.NopCloser(bytes.NewReader(prefetchBuffer))
		return rdr
	} else if prefetchCtx != nil {
		// prefetch in progress
		<-prefetchCtx.Done()

		f.mu.Lock()
		defer f.mu.Unlock()
		// check if prefetch was successful
		if f.prefetchBuffer != nil {
			rdr := io.NopCloser(bytes.NewReader(f.prefetchBuffer))
			f.prefetchBuffer = nil
			return rdr
		}
	}
	// no prefetch
	return nil
}

func (f *File) exist(ctx context.Context, fs storage.FS) bool {
	_, err := fs.Attributes(ctx, f.Path(), nil)
	if err != nil {
		return false
	}
	return true
}

func (f *File) existLegacy(ctx context.Context, fs storage.FS) bool {
	_, err := fs.Attributes(ctx, f.legacyPath(), nil)
	if err != nil {
		return false
	}
	return true
}

// ListFiles lists all ethwal files in the provided file system root directory
func ListFiles(ctx context.Context, fs storage.FS) ([]*File, error) {
	fileIndex := NewFileIndex(fs)

	err := fileIndex.Load(ctx)
	if err != nil {
		return nil, err
	}

	return fileIndex.Files(), nil
}

type FileIndex struct {
	fs storage.FS

	files []*File
}

func NewFileIndex(fs storage.FS) *FileIndex {
	return &FileIndex{fs: fs}
}

func NewFileIndexFromFiles(fs storage.FS, files []*File) *FileIndex {
	sort.Slice(files, func(i, j int) bool {
		return files[i].FirstBlockNum < files[j].FirstBlockNum
	})

	return &FileIndex{
		fs:    fs,
		files: files,
	}
}

func (fi *FileIndex) Files() []*File {
	return fi.files
}

func (fi *FileIndex) AddFile(file *File) error {
	_, _, err := fi.FindFile(file.FirstBlockNum)
	if err == nil {
		return fmt.Errorf("file already exist: block %d", file.FirstBlockNum)
	}

	fi.files = append(fi.files, file)
	return nil
}

func (fi *FileIndex) At(index int) *File {
	if index < 0 || index >= len(fi.files) {
		return nil
	}
	return fi.files[index]
}

func (fi *FileIndex) FindFile(blockNum uint64) (*File, int, error) {
	i := sort.Search(len(fi.files), func(i int) bool {
		return blockNum <= fi.files[i].LastBlockNum
	})
	if i == len(fi.files) {
		return nil, 0, ErrFileNotExist
	}
	return fi.files[i], i, nil
}

func (fi *FileIndex) IsLoaded() bool {
	return fi.files != nil
}

func (fi *FileIndex) Load(ctx context.Context) error {
	return fi.loadFiles(ctx)
}

func (fi *FileIndex) Save(ctx context.Context) error {
	// create file index file
	indexFile, err := fi.fs.Create(ctx, FileIndexFileName, nil)
	if err != nil {
		return err
	}

	comp := NewZSTDCompressor(indexFile)
	enc := NewCBOREncoder(comp)

	// close all resources
	closeAll := func() error {
		if err := comp.Close(); err != nil {
			_ = indexFile.Close()
			return err
		}
		return indexFile.Close()
	}

	// write all files
	for _, file := range fi.files {
		err = enc.Encode(file)
		if err != nil {
			_ = closeAll()
			return err
		}
	}
	return closeAll()
}

func (fi *FileIndex) loadFiles(ctx context.Context) error {
	// check if file index exists, if not migrate all existing ethwal files to the file index
	indexFile, openErr := fi.fs.Open(context.Background(), FileIndexFileName, nil)
	if openErr != nil && strings.Contains(openErr.Error(), "not exist") {
		// migrate all existing ethwal files to the file index
		migrationErr := migrateToFileIndex(ctx, fi.fs)
		if migrationErr != nil {
			return migrationErr
		}

		// open file index
		indexFile, openErr = fi.fs.Open(context.Background(), FileIndexFileName, nil)
		if openErr != nil && strings.Contains(openErr.Error(), "not exist") {
			// no files exist, so we return an empty list
			fi.files = []*File{}
			return nil
		}
	}
	if openErr != nil {
		return openErr
	}

	files, readErr := fi.readFiles(ctx, indexFile)
	if readErr != nil {
		_ = indexFile.Close()
		return readErr
	}

	fi.files = files
	return indexFile.Close()
}

func (fi *FileIndex) readFiles(ctx context.Context, rdr io.Reader) ([]*File, error) {
	var files []*File
	decomp := NewZSTDDecompressor(rdr)
	dec := NewCBORDecoder(decomp)

	for {
		var file File
		err := dec.Decode(&file)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			_ = decomp.Close()
			return nil, err
		}
		files = append(files, &file)
	}

	// remove last file if it does not exist, it may be incomplete due to crash
	if len(files) != 0 && !files[len(files)-1].Exist(ctx, fi.fs) {
		files = files[:len(files)-1]
	}

	if err := decomp.Close(); err != nil {
		return nil, err
	}
	return files, nil
}

// migrateToFileIndex migrates all ethwal files to the file index
func migrateToFileIndex(ctx context.Context, fs storage.FS) error {
	wlk, ok := fs.(storage.Walker)
	if !ok {
		return fmt.Errorf("ethwal: provided file system does not implement Walker interface")
	}

	var files []*File
	err := wlk.Walk(ctx, "", func(filePath string) error {
		// walk only wal files in current directory
		if path.Ext(filePath) != ".wal" || path.Dir(filePath) != "." {
			return nil
		}

		_, fileName := path.Split(filePath)
		firstBlockNum, lastBlockNum := parseWALFileBlockRange(fileName)
		files = append(files, &File{
			FirstBlockNum: firstBlockNum,
			LastBlockNum:  lastBlockNum,
		})
		return nil
	})
	if err != nil {
		return err
	}

	if len(files) == 0 {
		return nil
	}

	fileIndex := NewFileIndexFromFiles(fs, files)
	return fileIndex.Save(ctx)
}

// parseWALFileBlockRange reads first and last block number stored in WAL file from file name
func parseWALFileBlockRange(filePath string) (uint64, uint64) {
	_, fileName := path.Split(filePath)
	fileNameSplit := strings.Split(fileName, ".")
	blockNumberSplit := strings.Split(fileNameSplit[0], "_")

	first, _ := strconv.ParseInt(blockNumberSplit[0], 10, 64)
	last, _ := strconv.ParseInt(blockNumberSplit[1], 10, 64)

	return uint64(first), uint64(last)
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
