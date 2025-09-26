package ethwal

import (
	"context"
	"errors"
	"testing"

	"github.com/0xsequence/ethkit/go-ethereum/common"
	"github.com/0xsequence/ethwal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Mock writer for testing
type mockWriter[T any] struct {
	mock.Mock
}

func (m *mockWriter[T]) FileSystem() storage.FS {
	args := m.Called()
	return args.Get(0).(storage.FS)
}

func (m *mockWriter[T]) Write(ctx context.Context, b Block[T]) error {
	args := m.Called(ctx, b)
	return args.Error(0)
}

func (m *mockWriter[T]) BlockNum() uint64 {
	args := m.Called()
	return args.Get(0).(uint64)
}

func (m *mockWriter[T]) RollFile(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *mockWriter[T]) Close(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *mockWriter[T]) Options() Options {
	args := m.Called()
	return args.Get(0).(Options)
}

func (m *mockWriter[T]) SetOptions(opt Options) {
	m.Called(opt)
}

// Mock block hash getter for testing
type mockBlockHashGetter struct {
	mock.Mock
}

func (m *mockBlockHashGetter) GetHash(ctx context.Context, blockNum uint64) (common.Hash, error) {
	args := m.Called(ctx, blockNum)
	return args.Get(0).(common.Hash), args.Error(1)
}

func TestNewWriterWithVerifyHash(t *testing.T) {
	mockWriter := &mockWriter[int]{}
	mockGetter := &mockBlockHashGetter{}

	writer := NewWriterWithVerifyHash[int](mockWriter, mockGetter.GetHash)

	require.NotNil(t, writer)

	// Check that it returns a writerWithVerifyHash instance
	writerImpl, ok := writer.(*writerWithVerifyHash[int])
	require.True(t, ok)
	require.Equal(t, mockWriter, writerImpl.Writer)
	require.NotNil(t, writerImpl.blockHashGetter)
	require.Equal(t, common.Hash{}, writerImpl.prevHash)
}

func TestWriterWithVerifyHash_Write_FirstBlock(t *testing.T) {
	mockWriter := &mockWriter[int]{}
	mockGetter := &mockBlockHashGetter{}

	writer := NewWriterWithVerifyHash[int](mockWriter, mockGetter.GetHash)

	ctx := context.Background()
	block := Block[int]{
		Hash:   common.BytesToHash([]byte{0x01}),
		Parent: common.Hash{}, // First block has empty parent
		Number: 1,
		Data:   42,
	}

	// Mock expectations
	mockWriter.On("Write", ctx, block).Return(nil)

	err := writer.Write(ctx, block)

	require.NoError(t, err)
	mockWriter.AssertExpectations(t)
	mockGetter.AssertNotCalled(t, "GetHash") // Should not call getter for block 1

	// Check that prevHash is updated
	writerImpl := writer.(*writerWithVerifyHash[int])
	assert.Equal(t, block.Hash, writerImpl.prevHash)
}

func TestWriterWithVerifyHash_Write_SuccessfulSequence(t *testing.T) {
	mockWriter := &mockWriter[int]{}
	mockGetter := &mockBlockHashGetter{}

	writer := NewWriterWithVerifyHash[int](mockWriter, mockGetter.GetHash)

	ctx := context.Background()

	// First block
	block1 := Block[int]{
		Hash:   common.BytesToHash([]byte{0x01}),
		Parent: common.Hash{},
		Number: 1,
		Data:   42,
	}

	// Second block with correct parent
	block2 := Block[int]{
		Hash:   common.BytesToHash([]byte{0x02}),
		Parent: block1.Hash,
		Number: 2,
		Data:   43,
	}

	// Third block with correct parent
	block3 := Block[int]{
		Hash:   common.BytesToHash([]byte{0x03}),
		Parent: block2.Hash,
		Number: 3,
		Data:   44,
	}

	// Mock expectations
	mockWriter.On("Write", ctx, block1).Return(nil)
	mockWriter.On("Write", ctx, block2).Return(nil)
	mockWriter.On("Write", ctx, block3).Return(nil)

	// Write blocks in sequence
	err := writer.Write(ctx, block1)
	require.NoError(t, err)

	err = writer.Write(ctx, block2)
	require.NoError(t, err)

	err = writer.Write(ctx, block3)
	require.NoError(t, err)

	mockWriter.AssertExpectations(t)
	mockGetter.AssertNotCalled(t, "GetHash") // Should not call getter when writing sequentially

	// Check final prevHash state
	writerImpl := writer.(*writerWithVerifyHash[int])
	assert.Equal(t, block3.Hash, writerImpl.prevHash)
}

func TestWriterWithVerifyHash_Write_WithBlockHashGetter(t *testing.T) {
	mockWriter := &mockWriter[int]{}
	mockGetter := &mockBlockHashGetter{}

	writer := NewWriterWithVerifyHash[int](mockWriter, mockGetter.GetHash)

	ctx := context.Background()

	// Starting from block 3 (should fetch previous hash)
	prevHash := common.BytesToHash([]byte{0x02})
	block3 := Block[int]{
		Hash:   common.BytesToHash([]byte{0x03}),
		Parent: prevHash,
		Number: 3,
		Data:   44,
	}

	// Mock expectations
	mockGetter.On("GetHash", ctx, uint64(2)).Return(prevHash, nil)
	mockWriter.On("Write", ctx, block3).Return(nil)

	err := writer.Write(ctx, block3)

	require.NoError(t, err)
	mockWriter.AssertExpectations(t)
	mockGetter.AssertExpectations(t)

	// Check that prevHash is updated
	writerImpl := writer.(*writerWithVerifyHash[int])
	assert.Equal(t, block3.Hash, writerImpl.prevHash)
}

func TestWriterWithVerifyHash_Write_ParentHashMismatch(t *testing.T) {
	mockWriter := &mockWriter[int]{}
	mockGetter := &mockBlockHashGetter{}

	writer := NewWriterWithVerifyHash[int](mockWriter, mockGetter.GetHash)

	ctx := context.Background()

	// Set up a previous hash first
	writerImpl := writer.(*writerWithVerifyHash[int])
	writerImpl.prevHash = common.BytesToHash([]byte{0x01})

	// Block with wrong parent hash
	block := Block[int]{
		Hash:   common.BytesToHash([]byte{0x03}),
		Parent: common.BytesToHash([]byte{0x99}), // Wrong parent
		Number: 2,
		Data:   44,
	}

	err := writer.Write(ctx, block)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "parent hash mismatch")

	// Check that prevHash is reset to empty on error
	assert.Equal(t, common.Hash{}, writerImpl.prevHash)

	// Mock should not be called since validation fails first
	mockWriter.AssertNotCalled(t, "Write")
	mockGetter.AssertNotCalled(t, "GetHash")
}

func TestWriterWithVerifyHash_Write_BlockHashGetterError(t *testing.T) {
	mockWriter := &mockWriter[int]{}
	mockGetter := &mockBlockHashGetter{}

	writer := NewWriterWithVerifyHash[int](mockWriter, mockGetter.GetHash)

	ctx := context.Background()

	block := Block[int]{
		Hash:   common.BytesToHash([]byte{0x03}),
		Parent: common.BytesToHash([]byte{0x02}),
		Number: 3,
		Data:   44,
	}

	getterError := errors.New("failed to get block hash")

	// Mock expectations
	mockGetter.On("GetHash", ctx, uint64(2)).Return(common.Hash{}, getterError)

	err := writer.Write(ctx, block)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get block hash")
	assert.ErrorIs(t, err, getterError)

	mockGetter.AssertExpectations(t)
	mockWriter.AssertNotCalled(t, "Write") // Should not call write if getter fails
}

func TestWriterWithVerifyHash_Write_UnderlyingWriterError(t *testing.T) {
	mockWriter := &mockWriter[int]{}
	mockGetter := &mockBlockHashGetter{}

	writer := NewWriterWithVerifyHash[int](mockWriter, mockGetter.GetHash)

	ctx := context.Background()

	block := Block[int]{
		Hash:   common.BytesToHash([]byte{0x01}),
		Parent: common.Hash{},
		Number: 1,
		Data:   42,
	}

	writeError := errors.New("write failed")

	// Mock expectations
	mockWriter.On("Write", ctx, block).Return(writeError)

	err := writer.Write(ctx, block)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to write block")
	assert.ErrorIs(t, err, writeError)

	// Check that prevHash is reset to empty on write error
	writerImpl := writer.(*writerWithVerifyHash[int])
	assert.Equal(t, common.Hash{}, writerImpl.prevHash)

	mockWriter.AssertExpectations(t)
}

func TestWriterWithVerifyHash_Write_ParentHashMismatchAfterGetter(t *testing.T) {
	mockWriter := &mockWriter[int]{}
	mockGetter := &mockBlockHashGetter{}

	writer := NewWriterWithVerifyHash[int](mockWriter, mockGetter.GetHash)

	ctx := context.Background()

	// Block that requires fetching previous hash
	actualPrevHash := common.BytesToHash([]byte{0x02})
	wrongParentHash := common.BytesToHash([]byte{0x99})

	block := Block[int]{
		Hash:   common.BytesToHash([]byte{0x03}),
		Parent: wrongParentHash, // Wrong parent hash
		Number: 3,
		Data:   44,
	}

	// Mock expectations - getter returns correct hash, but block has wrong parent
	mockGetter.On("GetHash", ctx, uint64(2)).Return(actualPrevHash, nil)

	err := writer.Write(ctx, block)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "parent hash mismatch")

	// Check that prevHash is reset to empty on error
	writerImpl := writer.(*writerWithVerifyHash[int])
	assert.Equal(t, common.Hash{}, writerImpl.prevHash)

	mockGetter.AssertExpectations(t)
	mockWriter.AssertNotCalled(t, "Write") // Should not call write if validation fails
}

func TestWriterWithVerifyHash_InterfaceCompliance(t *testing.T) {
	// Test that all interface methods are properly delegated
	mockWriter := &mockWriter[int]{}
	mockGetter := &mockBlockHashGetter{}

	writer := NewWriterWithVerifyHash[int](mockWriter, mockGetter.GetHash)

	ctx := context.Background()

	// Test FileSystem delegation
	expectedFS := &struct{ storage.FS }{}
	mockWriter.On("FileSystem").Return(expectedFS)
	fs := writer.FileSystem()
	assert.Equal(t, expectedFS, fs)

	// Test BlockNum delegation
	mockWriter.On("BlockNum").Return(uint64(42))
	blockNum := writer.BlockNum()
	assert.Equal(t, uint64(42), blockNum)

	// Test RollFile delegation
	mockWriter.On("RollFile", ctx).Return(nil)
	err := writer.RollFile(ctx)
	assert.NoError(t, err)

	// Test Close delegation
	mockWriter.On("Close", ctx).Return(nil)
	err = writer.Close(ctx)
	assert.NoError(t, err)

	// Test Options delegation
	expectedOptions := Options{Dataset: Dataset{Name: "test"}}
	mockWriter.On("Options").Return(expectedOptions)
	options := writer.Options()
	assert.Equal(t, expectedOptions, options)

	// Test SetOptions delegation
	newOptions := Options{Dataset: Dataset{Name: "new-test"}}
	mockWriter.On("SetOptions", newOptions).Return()
	writer.SetOptions(newOptions)

	mockWriter.AssertExpectations(t)
}

func TestWriterWithVerifyHash_Write_ResetAfterError(t *testing.T) {
	// Test that the writer can recover after an error by resetting prevHash
	mockWriter := &mockWriter[int]{}
	mockGetter := &mockBlockHashGetter{}

	writer := NewWriterWithVerifyHash[int](mockWriter, mockGetter.GetHash)

	ctx := context.Background()
	writerImpl := writer.(*writerWithVerifyHash[int])

	// First, write a successful block
	block1 := Block[int]{
		Hash:   common.BytesToHash([]byte{0x01}),
		Parent: common.Hash{},
		Number: 1,
		Data:   42,
	}

	mockWriter.On("Write", ctx, block1).Return(nil)
	err := writer.Write(ctx, block1)
	require.NoError(t, err)
	assert.Equal(t, block1.Hash, writerImpl.prevHash)

	// Then try to write a block with wrong parent hash (should error and reset)
	block2Wrong := Block[int]{
		Hash:   common.BytesToHash([]byte{0x02}),
		Parent: common.BytesToHash([]byte{0x99}), // Wrong parent
		Number: 2,
		Data:   43,
	}

	err = writer.Write(ctx, block2Wrong)
	require.Error(t, err)
	assert.Equal(t, common.Hash{}, writerImpl.prevHash) // Should be reset

	// Now write a new sequence starting from block 3 (requiring hash lookup)
	block2Hash := common.BytesToHash([]byte{0x02})
	block3 := Block[int]{
		Hash:   common.BytesToHash([]byte{0x03}),
		Parent: block2Hash,
		Number: 3,
		Data:   44,
	}

	mockGetter.On("GetHash", ctx, uint64(2)).Return(block2Hash, nil)
	mockWriter.On("Write", ctx, block3).Return(nil)

	err = writer.Write(ctx, block3)
	require.NoError(t, err)
	assert.Equal(t, block3.Hash, writerImpl.prevHash)

	mockWriter.AssertExpectations(t)
	mockGetter.AssertExpectations(t)
}

func TestBlockGetterFromReader(t *testing.T) {
	// This test validates that the BlockGetterFromReader helper function can be created
	// The detailed functionality testing is covered in the integration tests above
	options := Options{
		Dataset: Dataset{
			Name:    "test-wal",
			Path:    testPath,
			Version: defaultDatasetVersion,
		},
		NewEncoder: NewJSONEncoder,
		NewDecoder: NewJSONDecoder,
	}

	getter := BlockHashGetterFromReader[int](options)
	require.NotNil(t, getter)

	// Test that calling it with invalid block returns error
	_, err := getter(context.Background(), 99)
	require.Error(t, err)
}
