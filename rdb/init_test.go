package rdb_test

import (
	"os"
)

const url = "sqlite.db"

func init() {
	if err := os.RemoveAll(url); err != nil {
		panic(err)
	}
}
