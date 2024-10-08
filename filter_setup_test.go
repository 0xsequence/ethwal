package ethwal

import (
	"context"
	"fmt"
	"os"

	"github.com/0xsequence/ethkit/go-ethereum/common"
	"github.com/0xsequence/ethkit/go-ethereum/common/math"
	"github.com/0xsequence/ethwal/storage"
	"github.com/0xsequence/ethwal/storage/local"
)

var (
	indexTestDir = ".tmp/ethwal_index_test"
)

func setupMockData[T any](subDir string, indexGenerator func() Indexes[T], blockGenerator func() []Block[T]) (*IndexBuilder[T], Indexes[T], storage.FS, func(), error) {
	fs := local.NewLocalFS(indexTestDir + "/" + subDir)
	indexes := indexGenerator()
	indexBuilder, err := NewIndexBuilder(indexes, fs)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	blocks := blockGenerator()
	for _, block := range blocks {
		err := indexBuilder.Index(context.Background(), block)
		if err != nil {
			return nil, nil, nil, nil, err
		}
	}
	err = indexBuilder.Flush(context.Background())
	if err != nil {
		return nil, nil, nil, nil, err
	}

	return indexBuilder, indexes, fs, cleanupIndexMockData(subDir), nil
}

func cleanupIndexMockData(subDir string) func() {
	return func() {
		err := os.RemoveAll(indexTestDir + "/" + subDir)
		if err != nil {
			panic(err)
		}
	}
}

func generateMixedIntBlocks() []Block[[]int] {
	blocks := []Block[[]int]{}

	// 0-19 generate 20 blocks with only even data
	// 20-39 generate 20 blocks with only odd data
	// 40-44 generate 5 blocks with even + odd data
	// 45-49 generate 5 blocks with no data
	// 50-69 generate 20 blocks with random but repeating huge numbers

	for i := 1; i <= 20; i++ {
		blocks = append(blocks, Block[[]int]{
			Hash:   common.BytesToHash([]byte{byte(i)}),
			Number: uint64(i),
			Data:   []int{i * 2},
		})
	}

	for i := 21; i <= 40; i++ {
		blocks = append(blocks, Block[[]int]{
			Hash:   common.BytesToHash([]byte{byte(i)}),
			Number: uint64(i),
			Data:   []int{i*2 + 1},
		})
	}

	for i := 41; i <= 45; i++ {
		blocks = append(blocks, Block[[]int]{
			Hash:   common.BytesToHash([]byte{byte(i)}),
			Number: uint64(i),
			Data:   []int{i*2 + 1, i*2 + 2},
		})
	}

	for i := 46; i <= 50; i++ {
		blocks = append(blocks, Block[[]int]{
			Hash:   common.BytesToHash([]byte{byte(i)}),
			Number: uint64(i),
			Data:   []int{},
		})
	}

	numbers := []int{
		121,
		123,
		125,
		999,
		777,
		333,
		555,
		111,
	}

	for i := 51; i < 71; i++ {
		data := []int{}
		for j := i; j < i+20; j++ {
			data = append(data, numbers[j%len(numbers)])
		}

		blocks = append(blocks, Block[[]int]{
			Hash:   common.BytesToHash([]byte{byte(i)}),
			Number: uint64(i),
			Data:   data,
		})
	}

	return blocks
}

func generateIntBlocks() []Block[[]int] {
	blocks := []Block[[]int]{}

	for i := 0; i < 100; i++ {
		blocks = append(blocks, Block[[]int]{
			Hash:   common.BytesToHash([]byte{byte(i)}),
			Number: uint64(i),
			Data:   []int{i, i + 1},
		})
	}

	return blocks
}

func generateMixedIntIndexes() Indexes[[]int] {
	indexes := Indexes[[]int]{}
	indexes["all"] = NewIndex[[]int]("all", indexBlock)
	indexes["odd_even"] = NewIndex[[]int]("odd_even", indexOddEvenBlocks)
	indexes["only_even"] = NewIndex[[]int]("only_even", indexOnlyEvenBlocks)
	indexes["only_odd"] = NewIndex[[]int]("only_odd", indexOnlyOddBlocks)
	return indexes
}

func generateIntIndexes() Indexes[[]int] {
	indexes := Indexes[[]int]{}
	indexes["all"] = NewIndex[[]int]("all", indexAll)
	indexes["none"] = NewIndex[[]int]("none", indexNone)
	return indexes
}

func indexOddEvenBlocks(block Block[[]int]) (toIndex bool, indexValueMap map[string][]uint16, err error) {
	if len(block.Data) == 0 {
		return false, nil, nil
	}

	toIndex = true
	indexValueMap = make(map[string][]uint16)
	indexValueMap["even"] = []uint16{}
	indexValueMap["odd"] = []uint16{}
	for i, data := range block.Data {
		if data%2 == 0 {
			indexValueMap["even"] = append(indexValueMap["even"], uint16(i))
		} else {
			indexValueMap["odd"] = append(indexValueMap["odd"], uint16(i))
		}
	}

	return
}

func indexOnlyEvenBlocks(block Block[[]int]) (toIndex bool, indexValueMap map[string][]uint16, err error) {
	if len(block.Data) == 0 {
		return false, nil, nil
	}

	toIndex = true
	indexValueMap = make(map[string][]uint16)
	for _, data := range block.Data {
		if data%2 != 0 {
			toIndex = false
			break
		}
	}

	if toIndex {
		indexValueMap["true"] = []uint16{math.MaxUint16}
	}

	return
}
func indexOnlyOddBlocks(block Block[[]int]) (toIndex bool, indexValueMap map[string][]uint16, err error) {
	if len(block.Data) == 0 {
		return false, nil, nil
	}

	toIndex = true
	indexValueMap = make(map[string][]uint16)
	for _, data := range block.Data {
		if data%2 == 0 {
			toIndex = false
			break
		}
	}

	if toIndex {
		indexValueMap["true"] = []uint16{math.MaxUint16}
	}

	return
}

func indexBlock(block Block[[]int]) (toIndex bool, indexValueMap map[string][]uint16, err error) {
	if len(block.Data) == 0 {
		return false, nil, nil
	}

	if block.Number < 50 {
		return false, nil, nil
	}

	toIndex = true
	indexValueMap = make(map[string][]uint16)
	for i, data := range block.Data {
		dataStr := fmt.Sprintf("%d", data)
		if _, ok := indexValueMap[dataStr]; !ok {
			indexValueMap[dataStr] = []uint16{}
		}
		indexValueMap[dataStr] = append(indexValueMap[dataStr], uint16(i))
	}

	return
}

func indexAll(block Block[[]int]) (toIndex bool, indexValueMap map[string][]uint16, err error) {
	if len(block.Data) == 0 {
		return false, nil, nil
	}

	toIndex = true
	indexValueMap = make(map[string][]uint16)
	for _, data := range block.Data {
		dataStr := fmt.Sprintf("%d", data)
		if _, ok := indexValueMap[dataStr]; !ok {
			indexValueMap[dataStr] = []uint16{math.MaxUint16}
		}
	}

	return
}

func indexNone(block Block[[]int]) (toIndex bool, indexValueMap map[string][]uint16, err error) {
	return false, nil, nil
}
