package main

import (
	"fmt"
	"io"
	"os"

	"github.com/0xsequence/ethwal"
	"github.com/0xsequence/ethwal/storage"
	"github.com/0xsequence/ethwal/storage/gcloud"
	"github.com/0xsequence/ethwal/storage/local"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/errgroup"
)

var SourceDatasetPathFlag = &cli.StringFlag{
	Name:     "src-path",
	Usage:    "source path to read",
	Required: true,
}

var SourceGoogleCloudBucket = &cli.StringFlag{
	Name:  "src-google-cloud-bucket",
	Usage: "source google cloud bucket",
}

var DestinationDatasetPathFlag = &cli.StringFlag{
	Name:     "dst-path",
	Usage:    "destination path to write",
	Required: true,
}

var DestinationGoogleCloudBucket = &cli.StringFlag{
	Name:  "dst-google-cloud-bucket",
	Usage: "estination google cloud bucket",
}

var ConcurrentWorkers = &cli.IntFlag{
	Name:  "workers",
	Usage: "number of concurrent workers",
	Value: 10,
}

func main() {
	app := cli.App{
		Name:  "ethwalcp",
		Usage: "tool to copy ethwal",
		Flags: []cli.Flag{
			SourceDatasetPathFlag,
			SourceGoogleCloudBucket,
			DestinationDatasetPathFlag,
			DestinationGoogleCloudBucket,
			ConcurrentWorkers,
		},
		Action: func(c *cli.Context) error {
			var srcFs storage.FS = local.NewLocalFS(c.String(SourceDatasetPathFlag.Name))
			if bucket := c.String(SourceGoogleCloudBucket.Name); bucket != "" {
				srcFs = gcloud.NewGCloudFS(bucket, nil)
				srcFs = storage.NewPrefixWrapper(srcFs, c.String(SourceDatasetPathFlag.Name))
			}

			var dstFs storage.FS = local.NewLocalFS(c.String(DestinationDatasetPathFlag.Name))
			if bucket := c.String(DestinationGoogleCloudBucket.Name); bucket != "" {
				dstFs = gcloud.NewGCloudFS(bucket, nil)
				dstFs = storage.NewPrefixWrapper(dstFs, c.String(DestinationDatasetPathFlag.Name))
			}

			errorGroup, gCtx := errgroup.WithContext(c.Context)

			fileList, err := ethwal.ListFiles(c.Context, srcFs)
			if err != nil {
				return fmt.Errorf("unable to list ethwal files: %w", err)
			}

			var filesChan = make(chan *ethwal.File, c.Int(ConcurrentWorkers.Name))
			errorGroup.Go(func() error {
				defer close(filesChan)
				for _, file := range fileList {
					select {
					case filesChan <- file:
					case <-gCtx.Done():
						return gCtx.Err()
					}
				}
				return nil
			})

			for i := 0; i < c.Int(ConcurrentWorkers.Name); i++ {
				errorGroup.Go(func() error {
					for file := range filesChan {
						if file.Exist(gCtx, dstFs) {
							fmt.Printf("File[%d-%d]: %s already exists, skipping\n", file.FirstBlockNum, file.LastBlockNum, file.Path())
							continue
						}

						fmt.Printf("Copying file[%d-%d]: %s\n", file.FirstBlockNum, file.LastBlockNum, file.Path())
						srcFile, err := file.Open(gCtx, srcFs)
						if err != nil {
							return fmt.Errorf("unable to open source file: %w", err)
						}
						closer := srcFile.Close

						dstFile, err := file.Create(gCtx, dstFs)
						if err != nil {
							_ = closer()
							return fmt.Errorf("unable to create destination file: %w", err)
						}
						closer = func() error {
							if err := srcFile.Close(); err != nil {
								_ = dstFile.Close()
								return err
							}
							return dstFile.Close()
						}

						_, err = io.Copy(dstFile, srcFile)
						if err != nil {
							_ = closer()
							return fmt.Errorf("unable to copy file: %w", err)
						}

						err = closer()
						if err != nil {
							return fmt.Errorf("unable to close file: %w", err)
						}
					}
					return nil
				})
			}

			if err := errorGroup.Wait(); err != nil {
				return fmt.Errorf("error copying files: %w", err)
			}

			fileIndexFile := ethwal.NewFileIndexFromFiles(dstFs, fileList)
			err = fileIndexFile.Save(gCtx)
			if err != nil {
				return fmt.Errorf("unable to save file index: %w", err)
			}

			fmt.Println("Copying complete")
			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error: %s\n", err.Error())
	}
}
