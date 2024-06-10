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

type LastBlockNumberRollPolicy interface {
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
	p.stats = &fileStats{}
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

type lastBlockNumberRollPolicy struct {
	rollInterval uint64

	lastBlockNum uint64
}

func NewLastBlockNumberRollPolicy(rollInterval uint64) LastBlockNumberRollPolicy {
	return &lastBlockNumberRollPolicy{rollInterval: rollInterval}
}

func (l *lastBlockNumberRollPolicy) ShouldRoll() bool {
	return l.lastBlockNum != 0 && l.lastBlockNum%l.rollInterval == 0
}

func (l *lastBlockNumberRollPolicy) Reset() {
	// noop
}

func (l *lastBlockNumberRollPolicy) LastBlockNum(blockNum uint64) {
	l.lastBlockNum = blockNum
}

type fileSizeOrLastBlockNumberRollPolicy struct {
	FileSizeRollPolicy
	LastBlockNumberRollPolicy
}

func NewFileSizeOrLastBlockNumberRollPolicy(maxSize, rollInterval uint64) FileRollPolicy {
	return &fileSizeOrLastBlockNumberRollPolicy{
		FileSizeRollPolicy:        NewFileSizeRollPolicy(maxSize),
		LastBlockNumberRollPolicy: NewLastBlockNumberRollPolicy(rollInterval),
	}
}

func (f *fileSizeOrLastBlockNumberRollPolicy) ShouldRoll() bool {
	return f.FileSizeRollPolicy.ShouldRoll() || f.LastBlockNumberRollPolicy.ShouldRoll()
}

func (f *fileSizeOrLastBlockNumberRollPolicy) Reset() {
	f.FileSizeRollPolicy.Reset()
	f.LastBlockNumberRollPolicy.Reset()
}
