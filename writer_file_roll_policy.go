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

	OnWrite(data []byte)
	OnBlockProcessed(blockNum uint64)
	OnFlush(ctx context.Context)
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

func (p *fileSizeRollPolicy) OnWrite(data []byte) {
	p.bytesWritten += uint64(len(data))
}

func (p *fileSizeRollPolicy) OnBlockProcessed(blockNum uint64) {}

func (p *fileSizeRollPolicy) OnFlush(ctx context.Context) {}

// fileStats is a writer that keeps track of the number of bytes written to it.
type writerWrapper struct {
	io.Writer

	fsrp FileRollPolicy
}

func (w *writerWrapper) Write(p []byte) (n int, err error) {
	defer w.fsrp.OnWrite(p)
	return w.Writer.Write(p)
}

type lastBlockNumberRollPolicy struct {
	rollInterval uint64

	lastBlockNum uint64
}

func (l *lastBlockNumberRollPolicy) OnWrite(data []byte) {}

func NewLastBlockNumberRollPolicy(rollInterval uint64) FileRollPolicy {
	return &lastBlockNumberRollPolicy{rollInterval: rollInterval}
}

func (l *lastBlockNumberRollPolicy) ShouldRoll() bool {
	return l.lastBlockNum != 0 && l.lastBlockNum%l.rollInterval == 0
}

func (l *lastBlockNumberRollPolicy) Reset() {
	// noop
}

func (l *lastBlockNumberRollPolicy) OnBlockProcessed(blockNum uint64) {
	l.lastBlockNum = blockNum
}

func (l *lastBlockNumberRollPolicy) OnFlush(ctx context.Context) {}

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

func (t *timeBasedRollPolicy) OnWrite(data []byte) {}

func (t *timeBasedRollPolicy) OnBlockProcessed(blockNum uint64) {}

func (t *timeBasedRollPolicy) OnFlush(ctx context.Context) {}

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

func (policies FileRollPolicies) OnWrite(data []byte) {
	for _, p := range policies {
		p.OnWrite(data)
	}
}

func (policies FileRollPolicies) OnBlockProcessed(blockNum uint64) {
	for _, p := range policies {
		p.OnBlockProcessed(blockNum)
	}
}

func (policies FileRollPolicies) OnFlush(ctx context.Context) {
	for _, p := range policies {
		p.OnFlush(ctx)
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

func (w *wrappedRollPolicy) OnWrite(data []byte) {
	w.rollPolicy.OnWrite(data)
}

func (w *wrappedRollPolicy) OnBlockProcessed(blockNum uint64) {
	w.rollPolicy.OnBlockProcessed(blockNum)
}

func (w *wrappedRollPolicy) OnFlush(ctx context.Context) {
	w.rollPolicy.OnFlush(ctx)
	w.flushFunc(ctx)
}

var _ FileRollPolicy = &fileSizeRollPolicy{}
