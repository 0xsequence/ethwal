package storage

import "github.com/Shopify/go-storage"

type FS storage.FS

type Walker storage.Walker

type File storage.File

var NewPrefixWrapper = storage.NewPrefixWrapper

var NewCacheWrapper = storage.NewCacheWrapper
