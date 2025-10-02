package ethwal

import (
	"context"
	"fmt"

	"github.com/0xsequence/ethkit/go-ethereum/common"
)

type BlockHashGetter func(ctx context.Context, blockNum uint64) (common.Hash, error)

func BlockHashGetterFromReader[T any](options Options) BlockHashGetter {
	return func(ctx context.Context, blockNum uint64) (common.Hash, error) {
		reader, err := NewReader[T](options)
		if err != nil {
			return common.Hash{}, fmt.Errorf("failed to create reader: %w", err)
		}
		defer reader.Close()

		err = reader.Seek(ctx, blockNum)
		if err != nil {
			return common.Hash{}, fmt.Errorf("failed to seek to block %d: %w", blockNum, err)
		}

		block, err := reader.Read(ctx)
		if err != nil {
			return common.Hash{}, fmt.Errorf("failed to read block %d: %w", blockNum, err)
		}
		return block.Hash, nil
	}
}

type writerWithVerifyHash[T any] struct {
	Writer[T]

	blockHashGetter BlockHashGetter

	prevHash common.Hash
}

var _ Writer[any] = (*writerWithVerifyHash[any])(nil)

func NewWriterWithVerifyHash[T any](writer Writer[T], blockHashGetter BlockHashGetter) Writer[T] {
	return &writerWithVerifyHash[T]{Writer: writer, blockHashGetter: blockHashGetter}
}

func (w *writerWithVerifyHash[T]) Write(ctx context.Context, b Block[T]) error {
	// Skip validation if block is first block
	if b.Number == 1 {
		if err := w.Writer.Write(ctx, b); err != nil {
			return fmt.Errorf("failed to write block: %w", err)
		}

		w.prevHash = b.Hash
		return nil
	}

	// Get previous hash if not already set
	if w.prevHash == (common.Hash{}) {
		prevHash, err := w.blockHashGetter(ctx, b.Number-1)
		if err != nil {
			return fmt.Errorf("failed to get block hash: %w", err)
		}

		w.prevHash = prevHash
	}

	// Validate parent hash
	if b.Parent != w.prevHash {
		w.prevHash = common.Hash{}
		return fmt.Errorf("parent hash mismatch, expected %s, got %s",
			w.prevHash.String(), b.Parent.String())
	}

	// Write block
	err := w.Writer.Write(ctx, b)
	if err != nil {
		w.prevHash = common.Hash{}
		return fmt.Errorf("failed to write block: %w", err)
	}

	// Update prev hash
	w.prevHash = b.Hash
	return nil
}
