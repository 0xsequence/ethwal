package ethwal

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"testing"

	"github.com/0xsequence/ethkit/go-ethereum/common"
	"github.com/c2h5oh/datasize"
	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const defaultDatasetVersion = "v1"

const testRoot = ".tmp"
const testPath = ".tmp/ethwal"

func testSetup(t *testing.T, newEncoder NewEncoderFunc, newCompressor NewCompressorFunc) {
	blocksFile1 := Blocks[int]{
		{
			Hash:   common.BytesToHash([]byte{0x01}),
			Number: 1,
			TS:     0,
			Data:   0,
		},
		{
			Hash:   common.BytesToHash([]byte{0x02}),
			Number: 2,
			TS:     0,
			Data:   0,
		},
		{
			Hash:   common.BytesToHash([]byte{0x03}),
			Number: 3,
			TS:     0,
			Data:   0,
		},
		{
			Hash:   common.BytesToHash([]byte{0x04}),
			Number: 4,
			TS:     0,
			Data:   0,
		},
	}

	blocksFile2 := Blocks[int]{
		{
			Hash:   common.BytesToHash([]byte{0x05}),
			Number: 5,
			TS:     0,
			Data:   0,
		},
		{
			Hash:   common.BytesToHash([]byte{0x06}),
			Number: 6,
			TS:     0,
			Data:   0,
		},
		{
			Hash:   common.BytesToHash([]byte{0x07}),
			Number: 7,
			TS:     0,
			Data:   0,
		},
		{
			Hash:   common.BytesToHash([]byte{0x08}),
			Number: 8,
			TS:     0,
			Data:   0,
		},
	}

	blocksFile3 := Blocks[int]{
		{
			Hash:   common.BytesToHash([]byte{0x0b}),
			Number: 11,
			TS:     0,
			Data:   0,
		},
		{
			Hash:   common.BytesToHash([]byte{0x0c}),
			Number: 12,
			TS:     0,
			Data:   0,
		},
	}

	walDir := path.Join(testPath, "int-wal", defaultDatasetVersion)
	_ = os.MkdirAll(walDir, 0755)

	f, err := os.OpenFile(path.Join(walDir, "1_4.wal"), os.O_CREATE|os.O_WRONLY, 0755)
	require.NoError(t, err)

	var w io.WriteCloser = f
	if newCompressor != nil {
		w = newCompressor(f)
	}

	enc := newEncoder(w)
	for _, blk := range blocksFile1 {
		_ = enc.Encode(blk)
	}
	_ = w.Close()

	f, err = os.OpenFile(path.Join(walDir, "5_8.wal"), os.O_CREATE|os.O_WRONLY, 0755)
	require.NoError(t, err)

	w = f
	if newCompressor != nil {
		w = newCompressor(f)
	}

	enc = newEncoder(w)
	for _, blk := range blocksFile2 {
		_ = enc.Encode(blk)
	}
	_ = w.Close()

	f, err = os.OpenFile(path.Join(walDir, "11_12.wal"), os.O_CREATE|os.O_WRONLY, 0755)
	require.NoError(t, err)

	w = f
	if newCompressor != nil {
		w = newCompressor(f)
	}

	enc = newEncoder(w)
	for _, blk := range blocksFile3 {
		_ = enc.Encode(blk)
	}
	_ = w.Close()
}

func testTeardown(t *testing.T) {
	fmt.Println("teardown")
	_ = os.RemoveAll(testRoot)
}

func TestReader_Read(t *testing.T) {
	testCase := []struct {
		name    string
		options Options
	}{
		{
			name: "json",
			options: Options{
				Dataset: Dataset{
					Name:    "int-wal",
					Path:    testPath,
					Version: defaultDatasetVersion,
				},
				NewEncoder: NewJSONEncoder,
				NewDecoder: NewJSONDecoder,
			},
		},
		{
			name: "json-zstd",
			options: Options{
				Dataset: Dataset{
					Name:    "int-wal",
					Path:    testPath,
					Version: defaultDatasetVersion,
				},
				NewEncoder:      NewJSONEncoder,
				NewDecoder:      NewJSONDecoder,
				NewCompressor:   NewZSTDCompressor,
				NewDecompressor: NewZSTDDecompressor,
			},
		},
		{
			name: "cbor",
			options: Options{
				Dataset: Dataset{
					Name:    "int-wal",
					Path:    testPath,
					Version: defaultDatasetVersion,
				},
				NewEncoder: NewCBOREncoder,
				NewDecoder: NewCBORDecoder,
			},
		},
		{
			name: "cbor-zstd",
			options: Options{
				Dataset: Dataset{
					Name:    "int-wal",
					Path:    testPath,
					Version: defaultDatasetVersion,
				},
				NewEncoder:      NewCBOREncoder,
				NewDecoder:      NewCBORDecoder,
				NewCompressor:   NewZSTDCompressor,
				NewDecompressor: NewZSTDDecompressor,
			},
		},
	}

	for _, tc := range testCase {
		t.Run(tc.name, func(t *testing.T) {
			testSetup(t, tc.options.NewEncoder, tc.options.NewCompressor)
			defer testTeardown(t)

			rdr, err := NewReader[int](tc.options)
			require.NoError(t, err)

			var blk Block[int]
			var blks []Block[int]
			for blk, err = rdr.Read(); err == nil; blk, err = rdr.Read() {
				//t.Logf("blk: %+v", blk)
				blks = append(blks, blk)
			}

			require.Equal(t, io.EOF, err)
			assert.Equal(t, 10, len(blks))

			assert.NoError(t, rdr.Close())
		})
	}
}

func TestReader_NumWALFiles(t *testing.T) {
	testSetup(t, NewCBOREncoder, nil)
	defer testTeardown(t)

	rdr, err := NewReader[int](Options{
		Dataset: Dataset{
			Name:    "int-wal",
			Path:    testPath,
			Version: defaultDatasetVersion,
		},
		NewEncoder: NewCBOREncoder,
		NewDecoder: NewCBORDecoder,
	})
	require.NoError(t, err)

	assert.Equal(t, 3, rdr.NumWALFiles())

	require.NoError(t, rdr.Close())
}

func TestReader_BlockNum(t *testing.T) {
	testSetup(t, NewCBOREncoder, nil)
	defer testTeardown(t)

	rdr, err := NewReader[int](Options{
		Dataset: Dataset{
			Name:    "int-wal",
			Path:    testPath,
			Version: defaultDatasetVersion,
		},
		NewEncoder: NewCBOREncoder,
		NewDecoder: NewCBORDecoder,
	})
	require.NoError(t, err)

	assert.Equal(t, uint64(0), rdr.BlockNum())

	blk, err := rdr.Read()
	require.NoError(t, err)

	assert.Equal(t, uint64(1), blk.Number)
	assert.Equal(t, uint64(1), rdr.BlockNum())

	blk, err = rdr.Read()
	require.NoError(t, err)

	assert.Equal(t, uint64(2), blk.Number)
	assert.Equal(t, uint64(2), rdr.BlockNum())

	err = rdr.Seek(5)
	require.NoError(t, err)
	assert.Equal(t, uint64(4), rdr.BlockNum()) // last block read was 4 next block is 5

	blk, err = rdr.Read()
	require.NoError(t, err)

	assert.Equal(t, uint64(5), blk.Number)
	assert.Equal(t, uint64(5), rdr.BlockNum())

	require.NoError(t, rdr.Close())
}

func TestReader_Seek(t *testing.T) {
	testSetup(t, NewCBOREncoder, nil)
	defer testTeardown(t)

	rdr, err := NewReader[int](Options{
		Dataset: Dataset{
			Name:    "int-wal",
			Path:    testPath,
			Version: defaultDatasetVersion,
		},
		NewEncoder: NewCBOREncoder,
		NewDecoder: NewCBORDecoder,
	})
	require.NoError(t, err)

	// seek to 2
	err = rdr.Seek(2)
	require.NoError(t, err)

	blk, err := rdr.Read()
	require.NoError(t, err)
	assert.Equal(t, uint64(2), blk.Number)

	blk, err = rdr.Read()
	require.NoError(t, err)
	assert.Equal(t, uint64(3), blk.Number)

	// seek to 10, which does not exist but there is a file with block 11
	err = rdr.Seek(10)
	require.NoError(t, err)

	blk, err = rdr.Read()
	require.NoError(t, err)
	assert.Equal(t, uint64(11), blk.Number)

	blk, err = rdr.Read()
	require.NoError(t, err)
	assert.Equal(t, uint64(12), blk.Number)

	_, err = rdr.Read()
	require.Equal(t, io.EOF, err)

	//  reader should return EOF on consecutive reads
	_, err = rdr.Read()
	require.Equal(t, io.EOF, err)

	// seek to 50 which does not exist and there is no file with block 50 or higher
	err = rdr.Seek(50)
	require.Equal(t, io.EOF, err)
}

func Test_ReaderStoragePathSuffix(t *testing.T) {
	defer testTeardown(t)

	options := Options{
		Dataset: Dataset{
			Name:    "int-wal",
			Path:    testPath,
			Version: defaultDatasetVersion,
		},
	}

	r, err := NewReader[int](options)
	require.NoError(t, err)
	reader, ok := r.(*reader[int])
	require.True(t, ok)
	require.Equal(t, string(reader.path[len(reader.path)-1]), string(os.PathSeparator))
}

func BenchmarkReaderInitializationTime(b *testing.B) {
	bench := []struct {
		FileNo uint64
	}{
		{
			FileNo: 2000,
		},
		{
			FileNo: 20000,
		},
		{
			FileNo: 200000,
		},
	}

	for _, benchCase := range bench {
		b.Run(fmt.Sprintf("FileNo-%d", benchCase.FileNo), func(b *testing.B) {
			defer func() { _ = os.RemoveAll(testRoot) }()

			rPath := path.Join(testPath, "v1")
			_ = os.MkdirAll(rPath, 0755)
			for i := uint64(0); i < benchCase.FileNo; i++ {
				fPAth := path.Join(rPath, fmt.Sprintf("%d_%d.wal", i+1, i+1))
				f, err := os.Create(fPAth)
				if err != nil {
					panic(err)
				}
				_, err = f.Write([]byte{0x01})
				if err != nil {
					panic(err)
				}
				err = f.Close()
				if err != nil {
					panic(err)
				}
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rdr, _ := NewReader[int](Options{
					Dataset: Dataset{
						Path: rPath,
					},
					NewDecoder:      NewCBORDecoder,
					NewDecompressor: NewZSTDDecompressor,
				})
				_ = rdr.Close()
			}
		})
	}
}

type File struct {
	StartBlock uint64 `cbor:"0,keyasint"`
	EndBlock   uint64 `cbor:"1,keyasint"`
}

func (f File) Path() string {
	hash := sha256.Sum256([]byte(fmt.Sprintf("%d_%d.wal", f.StartBlock, f.EndBlock)))
	return fmt.Sprintf("%x/%x/%x/%x", hash[0:8], hash[8:16], hash[16:24], hash[24:32])
}

func TestFilePath(t *testing.T) {
	f := File{1, 10000}

	spew.Dump(path.Join(testPath, f.Path()))
}

func BenchmarkReaderIndexFileLoadTime(b *testing.B) {
	bench := []struct {
		FileNo uint64
	}{
		{
			FileNo: 2000,
		},
		{
			FileNo: 20000,
		},
		{
			FileNo: 200000,
		},
		{
			FileNo: 2000000,
		},
	}

	for _, benchCase := range bench {
		b.Run(fmt.Sprintf("FileNo-%d", benchCase.FileNo), func(b *testing.B) {
			defer func() { _ = os.RemoveAll(testRoot) }()

			buff := bytes.NewBuffer(nil)
			comp := NewZSTDCompressor(buff)
			enc := NewCBOREncoder(comp)

			for i := uint64(0); i < benchCase.FileNo; i++ {
				_ = enc.Encode(File{
					StartBlock: i + 1,
					EndBlock:   i + 1,
				})
			}

			_ = comp.Close()

			rPath := path.Join(testPath, "v1")
			_ = os.MkdirAll(rPath, 0755)
			f, err := os.Create(path.Join(rPath, ".fileIndex"))
			if err != nil {
				panic(err)
			}

			buffBytes := buff.Bytes()

			_, err = f.Write(buffBytes)
			if err != nil {
				panic(err)
			}

			_ = f.Close()

			fmt.Println("indexFileSize", datasize.ByteSize(len(buffBytes)).HumanReadable())
			fmt.Println("theoreticalCapacity", (datasize.ByteSize(benchCase.FileNo) * 5 * datasize.MB).HumanReadable())

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var files []File

				f, err := os.Open(path.Join(rPath, ".fileIndex"))
				if err != nil {
					panic(err)
				}

				decomp := NewZSTDDecompressor(f)
				dec := NewCBORDecoder(decomp)
				for {
					var f File
					err = dec.Decode(&f)
					if errors.Is(err, io.EOF) {
						break
					} else if err != nil {
						panic(err)
					}

					files = append(files, f)
				}

				decomp.Close()
				f.Close()
			}
		})
	}
}
