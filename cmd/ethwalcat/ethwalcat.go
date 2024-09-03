package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"os"
	"strings"

	"github.com/0xsequence/ethkit/go-ethereum/common"
	"github.com/0xsequence/ethwal"
	"github.com/0xsequence/ethwal/storage"
	"github.com/0xsequence/ethwal/storage/gcloud"
	"github.com/urfave/cli/v2"
)

var ModeFlag = &cli.StringFlag{
	Name:  "mode",
	Usage: "mode to run in read/write",
}

var DatasetPathFlag = &cli.StringFlag{
	Name:     "path",
	Usage:    "path to read",
	Required: true,
}

var DatasetNameFlag = &cli.StringFlag{
	Name:  "name",
	Usage: "name of the dataset",
	Value: "",
}

var DatasetVersion = &cli.StringFlag{
	Name:  "version",
	Usage: "version of the dataset",
	Value: "",
}

var EncoderFlag = &cli.StringFlag{
	Name:  "encoder",
	Usage: "encoder to use",
	Value: "cbor",
}

var DecoderFlag = &cli.StringFlag{
	Name:  "decoder",
	Usage: "decoder to use",
	Value: "cbor",
}

var CompressorFlag = &cli.StringFlag{
	Name:  "compressor",
	Usage: "compressor to use",
	Value: "zstd",
}

var DecompressorFlag = &cli.StringFlag{
	Name:  "decompressor",
	Usage: "decompressor to use",
	Value: "zstd",
}

var FromBlockNumFlag = &cli.Uint64Flag{
	Name:  "from",
	Usage: "block number to start reading from",
	Value: 0,
}

var ToBlockNumFlag = &cli.Uint64Flag{
	Name:  "to",
	Usage: "block number to stop reading at",
	Value: 0,
}

var FileRollOnCloseFlag = &cli.BoolFlag{
	Name:  "file-roll-on-close",
	Usage: "roll on close",
	Value: true,
}

var GoogleCloudBucket = &cli.StringFlag{
	Name:  "google-cloud-bucket",
	Usage: "google cloud bucket",
}

func encoder(context *cli.Context) (ethwal.NewEncoderFunc, error) {
	switch context.String(EncoderFlag.Name) {
	case "cbor":
		return ethwal.NewCBOREncoder, nil
	case "json":
		return ethwal.NewJSONEncoder, nil
	default:
		return nil, fmt.Errorf("unknown encoder: %s", context.String(EncoderFlag.Name))
	}
}

func decoder(context *cli.Context) (ethwal.NewDecoderFunc, error) {
	switch context.String(DecoderFlag.Name) {
	case "cbor":
		return ethwal.NewCBORDecoder, nil
	case "json":
		return ethwal.NewJSONDecoder, nil
	default:
		return nil, fmt.Errorf("unknown decoder: %s", context.String(DecoderFlag.Name))
	}
}

func compressor(context *cli.Context) (ethwal.NewCompressorFunc, error) {
	switch context.String(CompressorFlag.Name) {
	case "zstd":
		return ethwal.NewZSTDCompressor, nil
	case "none":
		return nil, nil
	default:
		return nil, fmt.Errorf("unknown compressor: %s", context.String(CompressorFlag.Name))
	}
}

func decompressor(context *cli.Context) (ethwal.NewDecompressorFunc, error) {
	switch context.String(DecompressorFlag.Name) {
	case "zstd":
		return ethwal.NewZSTDDecompressor, nil
	case "none":
		return nil, nil
	default:
		return nil, fmt.Errorf("unknown decompressor: %s", context.String(DecompressorFlag.Name))
	}
}

func main() {
	app := cli.App{
		Name:  "ethwalcat",
		Usage: "tool to manage ethwal files",
		Flags: []cli.Flag{
			ModeFlag,
			DatasetPathFlag,
			DatasetNameFlag,
			DatasetVersion,
			EncoderFlag,
			DecoderFlag,
			CompressorFlag,
			DecompressorFlag,
			FromBlockNumFlag,
			ToBlockNumFlag,
			FileRollOnCloseFlag,
			GoogleCloudBucket,
		},
		Action: func(c *cli.Context) error {
			switch c.String(ModeFlag.Name) {
			case "read":
				dec, err := decoder(c)
				if err != nil {
					return err
				}

				decomp, err := decompressor(c)
				if err != nil {
					return err
				}

				var fs storage.FS
				if bucket := c.String(GoogleCloudBucket.Name); bucket != "" {
					fs = gcloud.NewGCloudFS(bucket, nil)
				}

				r, err := ethwal.NewReader[any](ethwal.Options{
					Dataset: ethwal.Dataset{
						Name:    c.String(DatasetNameFlag.Name),
						Version: c.String(DatasetVersion.Name),
						Path:    c.String(DatasetPathFlag.Name),
					},
					FileSystem:      fs,
					NewDecoder:      dec,
					NewDecompressor: decomp,
				})
				if err != nil {
					return err
				}

				if c.Uint64(FromBlockNumFlag.Name) > 0 {
					err = r.Seek(c.Context, c.Uint64(FromBlockNumFlag.Name))
					if err != nil {
						return err
					}
				}

				var toBlockNumber = c.Uint64(ToBlockNumFlag.Name)

				for b, err := r.Read(c.Context); err == nil; b, err = r.Read(c.Context) {
					if toBlockNumber != 0 && b.Number >= toBlockNumber {
						break
					}

					// cbor deserializes into map[interface{}]interface{} which can not be serialized into json
					if c.String(DecoderFlag.Name) == "cbor" {
						b.Data = normalizeDataFromCBOR(b.Data)
					}

					data, err := json.Marshal(b)
					if err != nil {

						return err
					}

					_, err = fmt.Fprintln(os.Stdout, string(data))
					if err != nil {
						return err
					}
				}

				if err != nil && err != io.EOF {
					return err
				}

				err = r.Close()
				if err != nil {
					return err
				}
			case "write":
				enc, err := encoder(c)
				if err != nil {
					return err
				}

				compres, err := compressor(c)
				if err != nil {
					return err
				}

				var fs storage.FS
				if bucket := c.String(GoogleCloudBucket.Name); bucket != "" {
					fs = gcloud.NewGCloudFS(bucket, nil)
				}

				w, err := ethwal.NewWriter[any](ethwal.Options{
					Dataset: ethwal.Dataset{
						Name:    c.String(DatasetNameFlag.Name),
						Version: c.String(DatasetVersion.Name),
						Path:    c.String(DatasetPathFlag.Name),
					},
					FileSystem:      fs,
					NewEncoder:      enc,
					NewCompressor:   compres,
					FileRollPolicy:  ethwal.NewFileSizeRollPolicy(uint64(8 << 20)), // 8 MB
					FileRollOnClose: c.Bool(FileRollOnCloseFlag.Name),
				})
				if err != nil {
					return err
				}

				in := bufio.NewReader(os.Stdin)
				for line, err := in.ReadString(byte('\n')); err == nil; line, err = in.ReadString(byte('\n')) {
					var b ethwal.Block[any]
					err = json.Unmarshal([]byte(line), &b)
					if err != nil {
						return err
					}

					// cbor needs to have hashes represented as byte slices
					if c.String(EncoderFlag.Name) == "cbor" {
						b.Data = normalizeDataToCBOR(b.Data)
					}

					err = w.Write(c.Context, b)
					if err != nil {
						return err
					}
				}

				if err != nil && err != io.EOF {
					return err
				}

				err = w.Close(c.Context)
				if err != nil {
					return err
				}
			default:
				return fmt.Errorf("unknown mode: %s", c.String(ModeFlag.Name))
			}

			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error: %s\n", err.Error())
	}
}

func normalizeDataFromCBOR(data any) any {
	if m, ok := data.(map[any]any); ok {
		var mr = make(map[string]any)
		for k, v := range m {
			mr[k.(string)] = normalizeDataFromCBOR(v)
		}
		return mr
	} else if arr, ok := data.([]any); ok {
		for i, v := range arr {
			arr[i] = normalizeDataFromCBOR(v)
		}
	} else if b, ok := data.([]byte); ok {
		if i, ok := big.NewInt(0).SetString(strings.ReplaceAll(string(b), "\"", ""), 10); ok {
			return i.String()
		}
		return fmt.Sprintf("0x%s", common.Bytes2Hex(b))
	}
	return data
}

func normalizeDataToCBOR(data any) any {
	if b, ok := data.(string); ok && strings.HasPrefix(b, "0x") {
		return common.Hex2Bytes(b)
	}
	return nil
}
