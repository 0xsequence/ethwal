# ethwal

A simple library for creating Ethereum based flat file datasets.

## Usage

The library consists of two main components, the `Writer` and the `Reader`. Each reader and writer needs to specify dataset name and path. The name is used to identify the dataset, and the path is used to specify the directory where the dataset is stored.
They may use ``json`` or ``cbor`` encoding for the data as well as ``zstd`` compression. The other custom encoders and compressors can be added by implementing the `ethwal.Encoder` and `ethwal.Compressor` interfaces.

The writer supports file roll over strategies such as: rolling over every Nth block, or after writing a certain amount of data.

### Writer

```go
package main

import (
	"fmt"

	"github.com/0xsequence/ethwal"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

func main() {
	w, err := ethwal.NewWriter[[]types.Log](ethwal.Options{
		Dataset: ethwal.Dataset{
			Name: "event-logs",
			Path: "data",
		},
		FileRollPolicy: ethwal.NewFileSizeRollPolicy(128 << 10), /* 128 KB */
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	for i := 0; i < 1000; i++ {
		err = w.Write(ethwal.Block[[]types.Log]{
			Number: uint64(i),
			Hash:   "0x123",
			Data: []types.Log{
				{
					Address: common.HexToAddress("0x123"),
					Topics: []common.Hash{
						common.HexToHash("0x123"),
					},
					Data: []byte("0x123"),
				},
			},
		})
		if err != nil {
			fmt.Println(err)
			return
		}
	}

	// Close the writer
	w.Close()
}
}
```

### Reader

```go
package main

import (
	"fmt"
	"io"

	"github.com/0xsequence/ethwal"
	"github.com/ethereum/go-ethereum/core/types"
)

func main() {
	r, err := ethwal.NewReader[[]types.Log](ethwal.Options{
		Dataset: ethwal.Dataset{
			Name: "event-logs",
			Path: "data",
		},
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	// Read all the blocks
	var b ethwal.Block[[]types.Log]
	for b, err = r.Read(); err == nil; b, err = r.Read() {
		fmt.Println(b.Number)
		fmt.Println(b.Hash)
		fmt.Println(b.Data)
	}

	if err != nil && err != io.EOF {
		fmt.Println(err)
		return
	}

	// Close the reader
	r.Close()
}
```