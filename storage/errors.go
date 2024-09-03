package storage

import "github.com/Shopify/go-storage"

func IsNotExist(err error) bool {
	return storage.IsNotExist(err)
}
