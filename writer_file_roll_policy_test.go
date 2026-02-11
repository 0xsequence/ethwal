package ethwal

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileSizeRollPolicy(t *testing.T) {
	var buff = bytes.NewBuffer(nil)

	p := NewFileSizeRollPolicy(10)
	w := writerWrapper{buff, p}

	_, err := w.Write([]byte("hello"))
	require.NoError(t, err)

	assert.False(t, p.ShouldRoll())

	_, err = w.Write([]byte(" world"))
	require.NoError(t, err)

	assert.True(t, p.ShouldRoll())

	p.Reset()
	w = writerWrapper{buff, p}
	assert.False(t, p.ShouldRoll())

	_, err = w.Write([]byte("hello world"))
	require.NoError(t, err)

	assert.True(t, p.ShouldRoll())

	p.ShouldRoll()
}

func TestLastBlockNumberRollPolicy(t *testing.T) {
	p := NewLastBlockNumberRollPolicy(10)
	assert.False(t, p.ShouldRoll())

	p.OnBlockProcessed(5)
	assert.False(t, p.ShouldRoll())

	p.OnBlockProcessed(10)
	assert.True(t, p.ShouldRoll())

	p.OnBlockProcessed(11)
	assert.False(t, p.ShouldRoll())
}

func TestTimeBasedRollPolicy(t *testing.T) {
	p := NewTimeBasedRollPolicy(1500*time.Millisecond, nil)
	assert.False(t, p.ShouldRoll())

	time.Sleep(1500 * time.Millisecond)
	assert.True(t, p.ShouldRoll())

	p.Reset()
	assert.False(t, p.ShouldRoll())

	time.Sleep(1500 * time.Millisecond)
	assert.True(t, p.ShouldRoll())
}

func TestNewFileSizeOrLastBlockNumberRollPolicy(t *testing.T) {
	var buff = bytes.NewBuffer(nil)

	fol := FileRollPolicies{
		NewFileSizeRollPolicy(10),
		NewLastBlockNumberRollPolicy(10),
	}

	w := writerWrapper{buff, fol}

	assert.False(t, fol.ShouldRoll())

	fol.OnBlockProcessed(10)
	assert.True(t, fol.ShouldRoll())

	fol.OnBlockProcessed(11)
	assert.False(t, fol.ShouldRoll())

	_, err := w.Write([]byte("hello world"))
	require.NoError(t, err)

	assert.True(t, fol.ShouldRoll())

	fol.Reset()
	assert.False(t, fol.ShouldRoll())

	fol.OnBlockProcessed(20)
	assert.True(t, fol.ShouldRoll())
}
