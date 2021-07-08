package tests

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	code := m.Run()
	// remove data
	for _, url := range testingUrl {
		os.RemoveAll(url)
	}
	os.Exit(code)
}
