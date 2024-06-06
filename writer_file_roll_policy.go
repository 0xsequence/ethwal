package ethlogwal

import "io"

type FileRollPolicy interface {
	ShouldRoll() bool
	Reset()
}

type FileSizeRollPolicy interface {
	FileRollPolicy

	WrapWriter(w io.Writer) io.Writer
}

type BlockNumberRollPolicy interface {
	FileRollPolicy

	LastBlockNum(blockNum uint64)
}

type fileSizeRollPolicy struct {
	maxSize uint64
	stats   *fileStats
}

func NewFileSizeRollPolicy(maxSize uint64) FileSizeRollPolicy {
	return &fileSizeRollPolicy{maxSize: maxSize}
}

func (p *fileSizeRollPolicy) WrapWriter(w io.Writer) io.Writer {
	p.stats = &fileStats{Writer: w}
	return p.stats
}

func (p *fileSizeRollPolicy) ShouldRoll() bool {
	return p.stats.BytesWritten >= p.maxSize
}

func (p *fileSizeRollPolicy) Reset() {
	// noop
}

// fileStats is a writer that keeps track of the number of bytes written to it.
type fileStats struct {
	io.Writer
	BytesWritten uint64
}

func (w *fileStats) Write(p []byte) (n int, err error) {
	n, err = w.Writer.Write(p)
	w.BytesWritten += uint64(n)
	return
}
