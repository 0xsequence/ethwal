package main

import (
	"cmp"
	"fmt"
	"os"

	"github.com/0xsequence/ethwal"
	"github.com/0xsequence/ethwal/storage"
	"github.com/0xsequence/ethwal/storage/gcloud"
	"github.com/0xsequence/ethwal/storage/local"
	"github.com/urfave/cli/v2"
)

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

var GoogleCloudBucket = &cli.StringFlag{
	Name:  "google-cloud-bucket",
	Usage: "google cloud bucket",
}

func main() {
	app := cli.App{
		Name:  "ethwalinfo",
		Usage: "tool to view ethwal",
		Flags: []cli.Flag{
			DatasetPathFlag,
			DatasetNameFlag,
			DatasetVersion,
			GoogleCloudBucket,
		},
		Action: func(c *cli.Context) error {
			var fs storage.FS = local.NewLocalFS("./")
			if bucket := c.String(GoogleCloudBucket.Name); bucket != "" {
				fs = gcloud.NewGCloudFS(bucket, nil)
			}

			dataset := ethwal.Dataset{
				Name:    c.String(DatasetNameFlag.Name),
				Version: c.String(DatasetVersion.Name),
				Path:    c.String(DatasetPathFlag.Name),
			}

			// mount fs to dataset path
			fs = storage.NewPrefixWrapper(fs, dataset.FullPath())

			walFiles, err := ethwal.ListFiles(c.Context, fs)
			if err != nil {
				return err
			}

			fmt.Println("Dataset:", cmp.Or(dataset.Name, "-"))
			fmt.Println("Version:", cmp.Or(dataset.Version, "-"))
			if c.String(GoogleCloudBucket.Name) != "" {
				fmt.Println("Filesystem:", "Google Cloud")
				fmt.Println("Bucket:", c.String(GoogleCloudBucket.Name))
			} else {
				fmt.Println("Filesystem: local")
			}
			fmt.Println("Path:", dataset.Path)
			fmt.Println("Number of files:", len(walFiles))
			if len(walFiles) > 0 {
				fmt.Println("Block range:", walFiles[0].FirstBlockNum, "-", walFiles[len(walFiles)-1].LastBlockNum)
			} else {
				fmt.Println("Block range: -")
			}

			return nil
		},
		Commands: []*cli.Command{
			{
				Name: "file_for_block",
				Args: true,
				Action: func(c *cli.Context) error {
					var fs storage.FS = local.NewLocalFS("./")
					if bucket := c.String(GoogleCloudBucket.Name); bucket != "" {
						fs = gcloud.NewGCloudFS(bucket, nil)
					}

					dataset := ethwal.Dataset{
						Name:    c.String(DatasetNameFlag.Name),
						Version: c.String(DatasetVersion.Name),
						Path:    c.String(DatasetPathFlag.Name),
					}

					// mount fs to dataset path
					fs = storage.NewPrefixWrapper(fs, dataset.FullPath())

					walFiles, err := ethwal.ListFiles(c.Context, fs)
					if err != nil {
						return err
					}

					var blockNum uint64
					if args := c.Args(); args.Len() == 1 {
						blockNumStr := args.Get(0)
						if _, err := fmt.Sscanf(blockNumStr, "%d", &blockNum); err != nil {
							return fmt.Errorf("invalid block number")
						}
					} else {
						return fmt.Errorf("invalid number of arguments")
					}

					for _, walFile := range walFiles {
						if blockNum >= walFile.FirstBlockNum && blockNum <= walFile.LastBlockNum {
							fmt.Println(walFile.Path())
							return nil
						}
					}
					return nil
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error: %s\n", err.Error())
	}
}
