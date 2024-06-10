# ethwal

A simple library for creating Ethereum based flat file datasets.

## Usage

The library consists of two main components, the `Writer` and the `Reader`. Each reader and writer needs to specify it's name and path. The name is used to identify the dataset, and the path is used to specify the directory where the dataset is stored.
They may use ``json`` or ``cbor`` encoding for the data as well as ``zstd`` compression. The other custom encoders and compressors can be added by implementing the `ethwal.Encoder` and `ethwal.Compressor` interfaces.

The writer supports file roll over strategies such as: rolling over every Nth block, or after writing a certain amount of data.

### Writer

```go
package main

import (
	"fmt"

	"github.com/0xsequence/ethkit/ethmonitor"
	"github.com/ethereum/go-ethereum/types"
)

func main() {
	w := ethwal.NewWriter[[]types.EventLog](ethwal.Options{
		Name:           "event-logs",
		Path:           "data",
		FileRollPolicy: ethwal.NewFileSizeRollPolicy(256), /* 256B */
	})

	err := &w.Write(ethwal.Block[[]types.EventLog]{
		Number: 1,
		Hash:   "0x123",
		Events: []types.EventLog{
			{
				Address: "0x123",
				Topics: []common.Hash{
					"0x123",
				},
				Data: []byte("0x123"),
			},
		},
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	...

	// Close the writer
	w.Close()
}
```

### Reader

```go
package main

import "github.com/0xsequence/ethkit/ethmonitor"

func main() {
	r := ethwal.NewReader[[]types.EventLog](ethwal.Options{
		Name: "event-logs",
		Path: "data",
	})

	// Read all the blocks
	var err error
	var b ethmonitor.Block[[]types.EventLog]
	for b, err = r.Read(); err == nil; b, err = r.Read() {
		fmt.Println(b.Number)
		fmt.Println(b.Hash)
		fmt.Println(b.Events)
	}
	
	if err != nil && err != io.EOF {
		fmt.Println(err)
		return
	}

	// Close the reader 
	r.Close()
}
```