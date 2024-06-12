package gcloud

import (
	"github.com/Shopify/go-storage"
	"golang.org/x/oauth2/google"
)

type GCloudFS struct {
	storage.FS
}

func NewGCloudFS(bucket string, credentials *google.Credentials) *GCloudFS {
	return &GCloudFS{FS: NewGoogleCloudChecksumStorage(
		storage.NewCloudStorageFS(bucket, credentials),
	)}
}
