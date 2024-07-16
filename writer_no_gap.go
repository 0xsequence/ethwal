package ethwal

type noGapWriter[T any] struct {
	w Writer[T]

	lastBlockNum uint64
}

func NewWriterNoGap[T any](w Writer[T]) Writer[T] {
	return &noGapWriter[T]{w: w}
}

func (n *noGapWriter[T]) Write(b Block[T]) error {
	defer func() { n.lastBlockNum = b.Number }()

	// skip if block number is less than or equal to last block number
	if b.Number <= n.lastBlockNum {
		return nil
	}

	// write blocks as there is no gap
	if b.Number == n.lastBlockNum+1 {
		return n.w.Write(b)
	}

	// write missing blocks
	for i := n.lastBlockNum + 1; i < b.Number; i++ {
		err := n.w.Write(Block[T]{Number: i})
		if err != nil {
			return err
		}
	}
	return n.w.Write(b)
}

func (n *noGapWriter[T]) RollFile() error {
	return n.w.RollFile()
}

func (n *noGapWriter[T]) BlockNum() uint64 {
	return n.w.BlockNum()
}

func (n *noGapWriter[T]) Close() error {
	return n.w.Close()
}
