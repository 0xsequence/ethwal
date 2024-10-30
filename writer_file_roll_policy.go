package ethwal

import (
	"context"
	"io"
	"sync"
	"time"
)

type FileRollPolicy interface {
	ShouldRoll() bool
	Reset()

	onWrite(data []byte)
	onBlockProcessed(blockNum uint64)
	onFlush(ctx context.Context)
}

type fileSizeRollPolicy struct {
	maxSize      uint64
	bytesWritten uint64
}

func NewFileSizeRollPolicy(maxSize uint64) FileRollPolicy {
	return &fileSizeRollPolicy{maxSize: maxSize}
}

func (p *fileSizeRollPolicy) ShouldRoll() bool {
	return p.bytesWritten >= p.maxSize
}

func (p *fileSizeRollPolicy) Reset() {
	p.bytesWritten = 0
}

func (p *fileSizeRollPolicy) onWrite(data []byte) {
	p.bytesWritten += uint64(len(data))
}

func (p *fileSizeRollPolicy) onBlockProcessed(blockNum uint64) {}

func (p *fileSizeRollPolicy) onFlush(ctx context.Context) {}

// fileStats is a writer that keeps track of the number of bytes written to it.
type writerWrapper struct {
	io.Writer

	fsrp FileRollPolicy
}

func (w *writerWrapper) Write(p []byte) (n int, err error) {
	defer w.fsrp.onWrite(p)
	return w.Writer.Write(p)
}

type lastBlockNumberRollPolicy struct {
	rollInterval uint64

	lastBlockNum uint64
}

func (l *lastBlockNumberRollPolicy) onWrite(data []byte) {}

func NewLastBlockNumberRollPolicy(rollInterval uint64) FileRollPolicy {
	return &lastBlockNumberRollPolicy{rollInterval: rollInterval}
}

func (l *lastBlockNumberRollPolicy) ShouldRoll() bool {
	return l.lastBlockNum != 0 && l.lastBlockNum%l.rollInterval == 0
}

func (l *lastBlockNumberRollPolicy) Reset() {
	// noop
}

func (l *lastBlockNumberRollPolicy) onBlockProcessed(blockNum uint64) {
	l.lastBlockNum = blockNum
}

func (l *lastBlockNumberRollPolicy) onFlush(ctx context.Context) {}

type timeBasedRollPolicy struct {
	rollInterval time.Duration
	onError      func(err error)

	rollFunc func() error

	bgCtx    context.Context
	bgCancel context.CancelFunc

	lastTimeRolled time.Time

	mu sync.Mutex
}

func NewTimeBasedRollPolicy(rollInterval time.Duration, onError func(err error)) FileRollPolicy {
	return &timeBasedRollPolicy{rollInterval: rollInterval, lastTimeRolled: time.Now(), onError: onError}
}

func (t *timeBasedRollPolicy) ShouldRoll() bool {
	if time.Since(t.lastTimeRolled) >= t.rollInterval {
		return true
	}
	return false
}

func (t *timeBasedRollPolicy) Reset() {
	t.lastTimeRolled = time.Now()
}

func (t *timeBasedRollPolicy) onWrite(data []byte) {}

func (t *timeBasedRollPolicy) onBlockProcessed(blockNum uint64) {}

func (t *timeBasedRollPolicy) onFlush(ctx context.Context) {}

type FileRollPolicies []FileRollPolicy

func (policies FileRollPolicies) ShouldRoll() bool {
	for _, p := range policies {
		if p.ShouldRoll() {
			return true
		}
	}
	return false
}

func (policies FileRollPolicies) Reset() {
	for _, p := range policies {
		p.Reset()
	}
}

func (policies FileRollPolicies) onWrite(data []byte) {
	for _, p := range policies {
		p.onWrite(data)
	}
}

func (policies FileRollPolicies) onBlockProcessed(blockNum uint64) {
	for _, p := range policies {
		p.onBlockProcessed(blockNum)
	}
}

func (policies FileRollPolicies) onFlush(ctx context.Context) {
	for _, p := range policies {
		p.onFlush(ctx)
	}
}

type wrappedRollPolicy struct {
	rollPolicy FileRollPolicy
	flushFunc  func(ctx context.Context)
}

func NewWrappedRollPolicy(rollPolicy FileRollPolicy, flushFunc func(ctx context.Context)) FileRollPolicy {
	return &wrappedRollPolicy{rollPolicy: rollPolicy, flushFunc: flushFunc}
}

func (w *wrappedRollPolicy) ShouldRoll() bool {
	return w.rollPolicy.ShouldRoll()
}

func (w *wrappedRollPolicy) Reset() {
	w.rollPolicy.Reset()
}

func (w *wrappedRollPolicy) onWrite(data []byte) {
	w.rollPolicy.onWrite(data)
}

func (w *wrappedRollPolicy) onBlockProcessed(blockNum uint64) {
	w.rollPolicy.onBlockProcessed(blockNum)
}

func (w *wrappedRollPolicy) onFlush(ctx context.Context) {
	w.rollPolicy.onFlush(ctx)
	w.flushFunc(ctx)
}

var _ FileRollPolicy = &fileSizeRollPolicy{}
