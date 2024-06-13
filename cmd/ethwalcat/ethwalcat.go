package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"

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

func mode(context *cli.Context) string {
	if context.String(DatasetPathFlag.Name) != "" {
		return "read"
	}
	return "write"
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
		Action: func(context *cli.Context) error {
			switch context.String(ModeFlag.Name) {
			case "read":
				dec, err := decoder(context)
				if err != nil {
					return err
				}

				decomp, err := decompressor(context)
				if err != nil {
					return err
				}

				var fs storage.FS
				if bucket := context.String(GoogleCloudBucket.Name); bucket != "" {
					fs = gcloud.NewGCloudFS(bucket, nil)
				}

				r, err := ethwal.NewReader[any](ethwal.Options{
					Dataset: ethwal.Dataset{
						Name:    context.String(DatasetNameFlag.Name),
						Version: context.String(DatasetVersion.Name),
						Path:    context.String(DatasetPathFlag.Name),
					},
					FileSystem:      fs,
					NewDecoder:      dec,
					NewDecompressor: decomp,
				})
				if err != nil {
					return err
				}

				if context.Uint64(FromBlockNumFlag.Name) > 0 {
					err = r.Seek(context.Uint64(FromBlockNumFlag.Name))
					if err != nil {
						return err
					}
				}

				var toBlockNumber = context.Uint64(ToBlockNumFlag.Name)

				for b, err := r.Read(); err == nil; b, err = r.Read() {
					if toBlockNumber != 0 && b.Number >= toBlockNumber {
						break
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
				enc, err := encoder(context)
				if err != nil {
					return err
				}

				compres, err := compressor(context)
				if err != nil {
					return err
				}

				var fs storage.FS
				if bucket := context.String(GoogleCloudBucket.Name); bucket != "" {
					fs = gcloud.NewGCloudFS(bucket, nil)
				}

				w, err := ethwal.NewWriter[any](ethwal.Options{
					Dataset: ethwal.Dataset{
						Name:    context.String(DatasetNameFlag.Name),
						Version: context.String(DatasetVersion.Name),
						Path:    context.String(DatasetPathFlag.Name),
					},
					FileSystem:      fs,
					NewEncoder:      enc,
					NewCompressor:   compres,
					FileRollPolicy:  ethwal.NewFileSizeRollPolicy(uint64(8 << 20)), // 8 MB
					FileRollOnClose: context.Bool(FileRollOnCloseFlag.Name),
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

					err = w.Write(b)
					if err != nil {
						return err
					}
				}

				if err != nil && err != io.EOF {
					return err
				}

				err = w.Close()
				if err != nil {
					return err
				}
			default:
				return fmt.Errorf("unknown mode: %s", mode(context))
			}

			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error: %s\n", err.Error())
	}
}
