# ethwal

A simple library for creating Ethereum based flat file datasets.

## Usage

The library consists of two main components, `Writer` and `Reader`. Each reader and writer needs to specify dataset name and path. The name is used to identify the dataset, and the path is used to specify the directory where the dataset is stored.
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

## CLI examples

### Read ethwal from local fs
```bash
$ ./ethwalcat --mode=read --path=./../indexer-data/db-logwal-new/137/v3/ --from=20000001 --to=20000005 --decompressor=zstd
{"blockHash":"0x90220f1f2d13248bef5ed31739b3625cb3696061ce891070ee7768dd6f94474f","blockNum":20000001,"blockTS":1633732467,"blockData":null}
{"blockHash":"0xb1db1d89202cee5cbc99f537ee0107e0d175d7f458e5c566657b644a3b205843","blockNum":20000002,"blockTS":1633732469,"blockData":null}
{"blockHash":"0x1c475400153bcfb8c1558cadc4fa94ed00ba28282fd63f13e0193e7a5e651d20","blockNum":20000003,"blockTS":1633732471,"blockData":null}
{"blockHash":"0xed17a5cfa53dffe8489c2f0dbea7d64cf732b3ef1d235ba3438a41154935b110","blockNum":20000004,"blockTS":1633732473,"blockData":null}
```

### Transcode ethwal from local cbor zstd to local json not compressed
```bash
./ethwalcat --mode=read --path=./../indexer-data/db-logwal-new/137/v3/ --from=20000001 --to=20000005 --decompressor=zstd | ./ethwalcat --mode=write --path=./ --encoder=json --compressor=none
```

### Read transcoded ethwal
```bash
$ ./ethwalcat --mode=read --path=./ --decoder=json --decompressor=none
{"blockHash":"0x90220f1f2d13248bef5ed31739b3625cb3696061ce891070ee7768dd6f94474f","blockNum":20000001,"blockTS":1633732467,"blockData":null}
{"blockHash":"0xb1db1d89202cee5cbc99f537ee0107e0d175d7f458e5c566657b644a3b205843","blockNum":20000002,"blockTS":1633732469,"blockData":null}
{"blockHash":"0x1c475400153bcfb8c1558cadc4fa94ed00ba28282fd63f13e0193e7a5e651d20","blockNum":20000003,"blockTS":1633732471,"blockData":null}
{"blockHash":"0xed17a5cfa53dffe8489c2f0dbea7d64cf732b3ef1d235ba3438a41154935b110","blockNum":20000004,"blockTS":1633732473,"blockData":null}
```

### Read ethwal from Google Cloud Bucket
```bash
$ ./ethwalcat --mode=read --google-cloud-bucket=sequence-dev-cluster-indexer-wal --path=./polygon-db-logwal/137/v2 --decompressor=zstd --from=1455120 --to=1455130
{"blockHash":"0x0000000000000000000000000000000000000000000000000000000000000000","blockNum":1455120,"blockTS":0,"blockData":null}
```
