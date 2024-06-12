package ethwal

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileSizeRollPolicy(t *testing.T) {
	var p FileSizeRollPolicy
	var buff = bytes.NewBuffer(nil)

	p = NewFileSizeRollPolicy(10)
	w := p.WrapWriter(buff)

	_, err := w.Write([]byte("hello"))
	require.NoError(t, err)

	assert.False(t, p.ShouldRoll())

	_, err = w.Write([]byte(" world"))
	require.NoError(t, err)

	assert.True(t, p.ShouldRoll())

	p.Reset()
	w = p.WrapWriter(buff)
	assert.False(t, p.ShouldRoll())

	_, err = w.Write([]byte("hello world"))
	require.NoError(t, err)

	assert.True(t, p.ShouldRoll())

	p.ShouldRoll()
}

func TestLastBlockNumberRollPolicy(t *testing.T) {
	var p LastBlockNumberRollPolicy

	p = NewLastBlockNumberRollPolicy(10)
	assert.False(t, p.ShouldRoll())

	p.LastBlockNum(5)
	assert.False(t, p.ShouldRoll())

	p.LastBlockNum(10)
	assert.True(t, p.ShouldRoll())

	p.LastBlockNum(11)
	assert.False(t, p.ShouldRoll())
}

func TestNewFileSizeOrLastBlockNumberRollPolicy(t *testing.T) {
	var buff = bytes.NewBuffer(nil)

	fol := NewFileSizeOrLastBlockNumberRollPolicy(10, 10)

	fs := fol.(FileSizeRollPolicy)
	lb := fol.(LastBlockNumberRollPolicy)

	require.NotNil(t, fs)
	require.NotNil(t, lb)

	w := fs.WrapWriter(buff)

	assert.False(t, fs.ShouldRoll())

	lb.LastBlockNum(10)
	assert.True(t, lb.ShouldRoll())

	lb.LastBlockNum(11)
	assert.False(t, lb.ShouldRoll())

	_, err := w.Write([]byte("hello world"))
	require.NoError(t, err)

	assert.True(t, fs.ShouldRoll())

	fs.Reset()
	assert.False(t, fs.ShouldRoll())

	lb.LastBlockNum(20)
	assert.True(t, lb.ShouldRoll())
}
