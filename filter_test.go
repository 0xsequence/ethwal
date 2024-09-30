package ethwal

import (
	"context"
	"fmt"
	"testing"

	"github.com/0xsequence/ethkit/go-ethereum/common"
	"github.com/0xsequence/ethkit/go-ethereum/core/types"
	"github.com/0xsequence/ethwal/storage/local"
	"github.com/stretchr/testify/require"
)

func Test_FilterWriter(t *testing.T) {
	reader, err := NewReader[[]types.Log](Options{
		Dataset: Dataset{
			Path: "../indexer-data/ethwal/137/ethlog/v1/",
		},
		NewDecompressor: NewZSTDDecompressor,
		NewDecoder:      NewCBORDecoder,
	})
	require.NoError(t, err)

	writer, err := NewWriter[[]types.Log](Options{
		Dataset: Dataset{
			Path: "../indexer-data/ethwal/137/ethlog/v2/",
		},
		NewDecompressor: NewZSTDDecompressor,
		NewDecoder:      NewCBORDecoder,
	})
	require.NoError(t, err)
	spongeboiIndexValues := map[string]struct{}{}
	transferWethIndexValues := map[string]struct{}{}
	indexes := Indexes[[]types.Log]{
		"spongeboi_erc_20_transfers_idx": NewIndex[[]types.Log]("spongeboi_erc_20_transfers_idx", func(block Block[[]types.Log]) (toIndex bool, indexValues []string, pos []uint16, err error) {
			pos = []uint16{}
			toIndex = false
			indexValues = []string{}
			err = nil
			for i, log := range block.Data {
				// if log.Address.String() == addy.String() {
				if len(log.Topics) == 3 && log.Topics[0].String() == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" &&
					(log.Topics[1].String() == "0x000000000000000000000000d4bbf5d234cc95441a8af0a317d8874ee425e74d" || log.Topics[2].String() == "0x000000000000000000000000d4bbf5d234cc95441a8af0a317d8874ee425e74d") {
					toIndex = true
					indexValue := log.Topics[0].Hex()
					indexValues = append(indexValues, indexValue)
					pos = append(pos, uint16(i))
					spongeboiIndexValues[indexValue] = struct{}{}
					// fmt.Println("spongeboi indexValue", indexValue)
				}
			}
			return
		}),
		"transfer_weth": NewIndex[[]types.Log]("transfer_weth", func(block Block[[]types.Log]) (toIndex bool, indexValues []string, pos []uint16, err error) {
			pos = []uint16{}
			toIndex = false
			indexValues = []string{}
			err = nil
			addy := common.HexToAddress("0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619")
			for i, log := range block.Data {
				if log.Address.String() == addy.String() {
					toIndex = true
					indexValue := string(log.Topics[0].Hex())
					pos = append(pos, uint16(i))
					indexValues = append(indexValues, indexValue)
					transferWethIndexValues[indexValue] = struct{}{}
					// fmt.Println("transfer_weth indexValue", indexValue)
				}
			}
			return
		}),
	}

	chainLensWriter, err := NewWriterWithIndexBuilder(writer, indexes)
	require.NoError(t, err)

	for {
		block, err := reader.Read(context.Background())
		// fmt.Println("reading block", block.Number)
		if err != nil {
			break
		}

		chainLensWriter.Write(context.Background(), block)
	}

	chainLensWriter.Close(context.Background())

	for indexValue := range spongeboiIndexValues {
		fmt.Println("spongeboi indexValue", indexValue)
	}
	for indexValue := range transferWethIndexValues {
		fmt.Println("transfer_weth indexValue", indexValue)
	}
}

func Test_Filter(t *testing.T) {
	fs := local.NewLocalFS("./indexes/")
	// fs.Create(context.Background(), "lmao_testing", nil)
	// reader, err := NewReader[[]types.Log](Options{
	// 	Dataset: Dataset{
	// 		Path: "../indexer-data/ethwal/137/ethlog/v1/",
	// 	},
	// 	NewDecompressor: NewZSTDDecompressor,
	// 	NewDecoder:      NewCBORDecoder,
	// })
	// require.NoError(t, err)
	indexes := Indexes[[]types.Log]{
		"spongeboi_erc_20_transfers_idx": NewIndex[[]types.Log]("spongeboi_erc_20_transfers_idx", func(block Block[[]types.Log]) (toIndex bool, indexValues []string, pos []uint16, err error) {
			return
		}),
		"transfer_weth": NewIndex[[]types.Log]("transfer_weth", func(block Block[[]types.Log]) (toIndex bool, indexValues []string, pos []uint16, err error) {
			return
		}),
	}
	fb, err := NewIndexesFilterBuilder(indexes, fs)
	require.NoError(t, err)

	spongeboiTxnFilter := fb.Eq("spongeboi_erc_20_transfers_idx", "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
	wethTransferFilter := fb.Eq("transfer_weth", "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
	f := fb.And(spongeboiTxnFilter, wethTransferFilter)
	// chainLensReader, err := NewChainLensReader(reader, f)
	iter := f.Eval()
	bmap := iter.Bitmap()
	fmt.Println("bmap len", bmap.GetCardinality())
	for itere := bmap.Iterator(); itere.HasNext(); {
		fmt.Println(IndexCompoundID(itere.Next()).Split())
	}
}

// topic hash for TxExcuted in sequence: 0x5c4eeb02dabf8976016ab414d617f9a162936dcace3cdef8c69ef6e262ad5ae7
// topic hash for Transfer in weth: 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef
