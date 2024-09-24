package main

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/0xsequence/ethkit/go-ethereum/common"
	"github.com/0xsequence/ethkit/go-ethereum/core/types"
	"github.com/0xsequence/ethwal"
	"github.com/Shopify/go-storage"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/cors"
)

type LogLocation struct {
	BlockNum uint64 `json:"blockNum"`
	LogIndex uint16 `json:"logIndex"`
}

func main() {
	var indexes = ethwal.Indexes[[]types.Log]{
		ethwal.IndexName("hasData").Normalize(): ethwal.NewIndex("hasData", func(block ethwal.Block[[]types.Log]) (toIndex bool, indexValues []string, positions []uint16, err error) {
			if len(block.Data) == 0 {
				toIndex = false
				return
			}

			toIndex = true
			indexValues = []string{"true"}
			positions = []uint16{0} // todo: magic number needed for all
			return
		}),
		ethwal.IndexName("contractAddress").Normalize(): ethwal.NewIndex("contractAddress", func(block ethwal.Block[[]types.Log]) (toIndex bool, indexValues []string, positions []uint16, err error) {
			toIndex = true
			positions = make([]uint16, 0, len(block.Data))
			indexValues = make([]string, 0, len(block.Data))

			for index, log := range block.Data {
				indexValues = append(indexValues, strings.ToLower(log.Address.Hex()))
				positions = append(positions, uint16(index))
			}
			return
		}),
		ethwal.IndexName("topic0").Normalize(): ethwal.NewIndex("topic0", func(block ethwal.Block[[]types.Log]) (toIndex bool, indexValues []string, positions []uint16, err error) {
			toIndex = true
			positions = make([]uint16, 0, len(block.Data))
			indexValues = make([]string, 0, len(block.Data))

			for index, log := range block.Data {
				if len(log.Topics) < 1 || log.Topics[0].Cmp(common.Hash{}) == 0 {
					continue
				}

				indexValues = append(indexValues, strings.ToLower(log.Topics[0].Hex()))
				positions = append(positions, uint16(index))
			}

			if len(indexValues) == 0 {
				toIndex = false
			}
			return
		}),
		ethwal.IndexName("topic1").Normalize(): ethwal.NewIndex("topic1", func(block ethwal.Block[[]types.Log]) (toIndex bool, indexValues []string, positions []uint16, err error) {
			toIndex = true
			positions = make([]uint16, 0, len(block.Data))
			indexValues = make([]string, 0, len(block.Data))

			for index, log := range block.Data {
				if len(log.Topics) < 2 || log.Topics[1].Cmp(common.Hash{}) == 0 {
					continue
				}

				indexValues = append(indexValues, strings.ToLower(log.Topics[1].Hex()))
				positions = append(positions, uint16(index))
			}

			if len(indexValues) == 0 {
				toIndex = false
			}
			return
		}),
		ethwal.IndexName("topic2").Normalize(): ethwal.NewIndex("topic2", func(block ethwal.Block[[]types.Log]) (toIndex bool, indexValues []string, positions []uint16, err error) {
			toIndex = true
			positions = make([]uint16, 0, len(block.Data))
			indexValues = make([]string, 0, len(block.Data))

			for index, log := range block.Data {
				if len(log.Topics) < 3 || log.Topics[2].Cmp(common.Hash{}) == 0 {
					continue
				}

				indexValues = append(indexValues, strings.ToLower(log.Topics[2].Hex()))
				positions = append(positions, uint16(index))
			}

			if len(indexValues) == 0 {
				toIndex = false
			}
			return
		}),
		ethwal.IndexName("topic3").Normalize(): ethwal.NewIndex("topic3", func(block ethwal.Block[[]types.Log]) (toIndex bool, indexValues []string, positions []uint16, err error) {
			toIndex = true
			positions = make([]uint16, 0, len(block.Data))
			indexValues = make([]string, 0, len(block.Data))

			for index, log := range block.Data {
				if len(log.Topics) < 4 || log.Topics[3].Cmp(common.Hash{}) == 0 {
					continue
				}

				indexValues = append(indexValues, strings.ToLower(log.Topics[3].Hex()))
				positions = append(positions, uint16(index))
			}

			if len(indexValues) == 0 {
				toIndex = false
			}
			return
		}),
	}

	fs := storage.NewLocalFS("/Volumes/FastAndFurious/Praca/0xsequence/indexer-data/ethwal/polygon/ethlog/v1/index/")

	httpServer := &http.Server{
		Addr:              ":8585",
		ReadTimeout:       45 * time.Second,
		IdleTimeout:       45 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
	}

	r := chi.NewRouter()

	r.Use(func(handler http.Handler) http.Handler {
		corsTrustedOptions := cors.Options{
			AllowedOrigins:   []string{"https://*", "http://*"},
			AllowedMethods:   []string{"HEAD", "GET", "POST", "OPTIONS"},
			AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "Release", "X-Access-Key", "X-Sequence-Token-Key"},
			ExposedHeaders:   []string{"Link"},
			AllowCredentials: true,
			MaxAge:           600,
		}

		corsTrustedOptions.AllowOriginFunc = func(r *http.Request, origin string) bool {
			return true
		}
		return cors.Handler(corsTrustedOptions)(handler)
	})

	r.Get("/GetBlockNumbers", func(w http.ResponseWriter, r *http.Request) {
		filterBuilder, err := ethwal.NewIndexesFilterBuilder(indexes, fs)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		var filters []ethwal.Filter
		contractAddress := r.URL.Query().Get("contractAddress")
		if contractAddress != "" {
			filters = append(filters, filterBuilder.Eq("contractAddress", strings.ToLower(contractAddress)))
		}

		topic0 := r.URL.Query().Get("topic0")
		if topic0 != "" {
			filters = append(filters, filterBuilder.Eq("topic0", strings.ToLower(topic0)))
		}

		hasData := r.URL.Query().Get("hasData")
		if hasData == "true" {
			filters = append(filters, filterBuilder.Eq("hasData", hasData))
		}

		filter := filterBuilder.And(filters...)

		var logLocations = []LogLocation{}
		if len(filters) == 0 {
			resp, err := json.Marshal(logLocations)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			w.WriteHeader(200)
			w.Write(resp)
			return
		}

		iter := filter.Eval()
		for iter.HasNext() {
			blockNumber, dataIndex := iter.Next()
			logLocations = append(logLocations, LogLocation{blockNumber, dataIndex + 1})
		}

		resp, err := json.Marshal(logLocations)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(200)
		w.Write(resp)
	})

	httpServer.Handler = r
	httpServer.ListenAndServe()
}
