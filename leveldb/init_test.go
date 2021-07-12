package leveldb_test

import (
	"os"
)

const url = "leveldb.db"

func init() {
	if err := os.RemoveAll(url); err != nil {
		panic(err)
	}
}
