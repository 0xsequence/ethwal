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

func TestBlockHashGetterFromReader_ReturnsCorrectHash(t *testing.T) {
	testSetup(t, NewJSONEncoder, nil)
	defer testTeardown(t)

	options := Options{
		Dataset: Dataset{
			Name:    "int-wal",
			Path:    testPath,
			Version: defaultDatasetVersion,
		},
		NewEncoder: NewJSONEncoder,
		NewDecoder: NewJSONDecoder,
	}

	ctx := context.Background()

	// Create the block hash getter from reader
	getter := BlockHashGetterFromReader[int](options)
	require.NotNil(t, getter)

	// Define expected blocks and their hashes (matches testSetup)
	expectedBlocks := []struct {
		blockNum uint64
		hash     common.Hash
	}{
		{1, common.BytesToHash([]byte{0x01})},
		{2, common.BytesToHash([]byte{0x02})},
		{3, common.BytesToHash([]byte{0x03})},
		{4, common.BytesToHash([]byte{0x04})},
		{5, common.BytesToHash([]byte{0x05})},
		{6, common.BytesToHash([]byte{0x06})},
		{7, common.BytesToHash([]byte{0x07})},
		{8, common.BytesToHash([]byte{0x08})},
		{11, common.BytesToHash([]byte{0x0b})},
		{12, common.BytesToHash([]byte{0x0c})},
	}

	// Test that the getter returns the correct hash for each block
	for _, expected := range expectedBlocks {
		hash, err := getter(ctx, expected.blockNum)
		require.NoError(t, err, "Failed to get hash for block %d", expected.blockNum)
		assert.Equal(t, expected.hash, hash,
			"Hash mismatch for block %d: expected %s, got %s",
			expected.blockNum, expected.hash.String(), hash.String())
	}
}

func TestBlockHashGetterFromReader_NonExistentBlock(t *testing.T) {
	testSetup(t, NewJSONEncoder, nil)
	defer testTeardown(t)

	options := Options{
		Dataset: Dataset{
			Name:    "int-wal",
			Path:    testPath,
			Version: defaultDatasetVersion,
		},
		NewEncoder: NewJSONEncoder,
		NewDecoder: NewJSONDecoder,
	}

	ctx := context.Background()
	getter := BlockHashGetterFromReader[int](options)
	require.NotNil(t, getter)

	// Test a block that definitely doesn't exist (way beyond the test data range)
	hash, err := getter(ctx, 1000000)
	require.Error(t, err, "Expected error for non-existent block")
	assert.Equal(t, common.Hash{}, hash, "Hash should be empty for non-existent block")
}

func TestBlockHashGetterFromReader_WithCompression(t *testing.T) {
	testSetup(t, NewJSONEncoder, NewZSTDCompressor)
	defer testTeardown(t)

	options := Options{
		Dataset: Dataset{
			Name:    "int-wal",
			Path:    testPath,
			Version: defaultDatasetVersion,
		},
		NewEncoder:      NewJSONEncoder,
		NewDecoder:      NewJSONDecoder,
		NewCompressor:   NewZSTDCompressor,
		NewDecompressor: NewZSTDDecompressor,
	}

	ctx := context.Background()
	getter := BlockHashGetterFromReader[int](options)
	require.NotNil(t, getter)

	// Test a few blocks to ensure compression doesn't affect hash retrieval
	testCases := []struct {
		blockNum uint64
		hash     common.Hash
	}{
		{1, common.BytesToHash([]byte{0x01})},
		{5, common.BytesToHash([]byte{0x05})},
		{11, common.BytesToHash([]byte{0x0b})},
	}

	for _, tc := range testCases {
		hash, err := getter(ctx, tc.blockNum)
		require.NoError(t, err, "Failed to get hash for block %d with compression", tc.blockNum)
		assert.Equal(t, tc.hash, hash,
			"Hash mismatch for block %d with compression: expected %s, got %s",
			tc.blockNum, tc.hash.String(), hash.String())
	}
}

func TestWriterWithVerifyHash_Write_BlockZero(t *testing.T) {
	defer testTeardown(t)

	options := Options{
		Dataset: Dataset{
			Name:    "test-block-zero",
			Path:    testPath,
			Version: defaultDatasetVersion,
		},
		NewEncoder:      NewJSONEncoder,
		NewDecoder:      NewJSONDecoder,
		FileRollOnClose: true,
	}

	ctx := context.Background()

	// Create a writer
	w, err := NewWriter[int](options)
	require.NoError(t, err)

	// Create a writerWithVerifyHash
	mockGetter := &mockBlockHashGetter{}
	verifyWriter := NewWriterWithVerifyHash[int](w, mockGetter.GetHash)

	// Write block 0
	block0 := Block[int]{
		Hash:   common.BytesToHash([]byte{0x00}),
		Parent: common.Hash{}, // Block 0 has empty parent
		Number: 0,
		Data:   100,
	}

	err = verifyWriter.Write(ctx, block0)
	require.NoError(t, err, "Block 0 should be written successfully")
	assert.Equal(t, uint64(0), w.BlockNum(), "BlockNum should return 0")

	// Write block 1 to ensure file gets flushed (writer needs at least one block >= firstBlockNum)
	block1 := Block[int]{
		Hash:   common.BytesToHash([]byte{0x01}),
		Parent: block0.Hash,
		Number: 1,
		Data:   101,
	}

	err = verifyWriter.Write(ctx, block1)
	require.NoError(t, err, "Block 1 should be written successfully")

	// Debug: Check writer state before close
	writerImpl := w.(*writer[int])
	t.Logf("Before close: firstBlockNum=%d, lastBlockNum=%d", writerImpl.firstBlockNum, writerImpl.lastBlockNum)

	// Close writer to flush to disk
	err = w.Close(ctx)
	require.NoError(t, err)

	// Debug: Check file index
	reader, err := NewReader[int](options)
	require.NoError(t, err)
	defer reader.Close()

	fileIndex := reader.FileIndex()
	files := fileIndex.Files()
	t.Logf("Number of files: %d", len(files))
	for i, f := range files {
		t.Logf("File %d: blocks %d-%d, path=%s", i, f.FirstBlockNum, f.LastBlockNum, f.Path())
	}

	// Verify we can read block 0 directly with the reader (already created above for debugging)
	err = reader.Seek(ctx, 0)
	require.NoError(t, err, "Should be able to seek to block 0")

	readBlock0, err := reader.Read(ctx)
	require.NoError(t, err, "Should be able to read block 0")
	assert.Equal(t, uint64(0), readBlock0.Number, "Block number should be 0")
	assert.Equal(t, block0.Hash, readBlock0.Hash, "Block 0 hash should match")
	assert.Equal(t, block0.Data, readBlock0.Data, "Block 0 data should match")

	// Read block 1
	readBlock1, err := reader.Read(ctx)
	require.NoError(t, err, "Should be able to read block 1")
	assert.Equal(t, uint64(1), readBlock1.Number, "Block number should be 1")
	assert.Equal(t, block1.Hash, readBlock1.Hash, "Block 1 hash should match")
	assert.Equal(t, block1.Data, readBlock1.Data, "Block 1 data should match")

	// Also verify using BlockHashGetterFromReader
	getter := BlockHashGetterFromReader[int](options)
	hash0, err := getter(ctx, 0)
	require.NoError(t, err, "Should be able to get block 0 hash via getter")
	assert.Equal(t, block0.Hash, hash0, "Block 0 hash from getter should match")

	hash1, err := getter(ctx, 1)
	require.NoError(t, err, "Should be able to get block 1 hash via getter")
	assert.Equal(t, block1.Hash, hash1, "Block 1 hash from getter should match")

	mockGetter.AssertNotCalled(t, "GetHash")
}

func TestBlockHashGetterFromReader(t *testing.T) {
	defer testTeardown(t)

	options := Options{
		Dataset: Dataset{
			Name:    "test-block-zero",
			Path:    testPath,
			Version: defaultDatasetVersion,
		},
		NewEncoder:      NewJSONEncoder,
		NewDecoder:      NewJSONDecoder,
		FileRollOnClose: true,
	}

	w, err := NewWriter[int](options)
	require.NoError(t, err)

	verifyWriter := NewWriterWithVerifyHash[int](w, BlockHashGetterFromReader[int](options))

	block0 := Block[int]{
		Hash:   common.BytesToHash([]byte{0x01}),
		Parent: common.Hash{0x00},
		Number: 0,
		Data:   100,
	}

	err = verifyWriter.Write(context.Background(), block0)
	require.NoError(t, err)

	block1 := Block[int]{
		Hash:   common.BytesToHash([]byte{0x02}),
		Parent: block0.Hash,
		Number: 1,
		Data:   101,
	}

	err = verifyWriter.Write(context.Background(), block1)
	require.NoError(t, err)

	err = w.RollFile(context.Background())
	require.NoError(t, err)

	err = w.Close(context.Background())
	require.NoError(t, err)

	r, err := NewReader[int](options)
	require.NoError(t, err)

	readBlock0, err := r.Read(context.Background())
	require.NoError(t, err)
	assert.Equal(t, block0.Hash, readBlock0.Hash)
	assert.Equal(t, block0.Data, readBlock0.Data)

	readBlock1, err := r.Read(context.Background())
	require.NoError(t, err)
	assert.Equal(t, block1.Hash, readBlock1.Hash)
	assert.Equal(t, block1.Data, readBlock1.Data)

	// continue writing with verify writer
	w, err = NewWriter[int](options)
	require.NoError(t, err)

	verifyWriter = NewWriterWithVerifyHash[int](w, BlockHashGetterFromReader[int](options))

	block3 := Block[int]{
		Hash:   common.BytesToHash([]byte{0x03}),
		Parent: block1.Hash,
		Number: 2,
		Data:   103,
	}

	err = verifyWriter.Write(context.Background(), block3)
	require.NoError(t, err)

	block4 := Block[int]{
		Hash:   common.BytesToHash([]byte{0x04}),
		Parent: block3.Hash,
		Number: 3,
		Data:   104,
	}

	err = verifyWriter.Write(context.Background(), block4)
	require.NoError(t, err)

	err = w.RollFile(context.Background())
	require.NoError(t, err)

	err = w.Close(context.Background())
	require.NoError(t, err)

	r, err = NewReader[int](options)
	require.NoError(t, err)

	var blocks = []Block[int]{block0, block1, block3, block4}
	for _, block := range blocks {
		readBlock, err := r.Read(context.Background())
		require.NoError(t, err)
		assert.Equal(t, block.Hash, readBlock.Hash)
		assert.Equal(t, block.Data, readBlock.Data)
	}

	require.NoError(t, r.Close())
}
