package leveldb_test

import (
	"testing"

	"github.com/elvinchan/kvdb"
	"github.com/elvinchan/kvdb/leveldb"
	"github.com/elvinchan/kvdb/tests"
)

func newDB() (kvdb.KVDB, error) {
	return leveldb.NewDB(url,
		kvdb.DefaultLimit(tests.DefaultLimit))
}

func TestNewDB(t *testing.T) {
	db, err := newDB()
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	if db == nil {
		t.Errorf("db is nil")
		t.Fail()
	}
	err = db.Close()
	if err != nil {
		t.Error(err)
		t.Fail()
	}
}

func TestGetSet(t *testing.T) {
	tests.TestGetSet(t, newDB)
}

func TestDelete(t *testing.T) {
	tests.TestDelete(t, newDB)
}

func TestExist(t *testing.T) {
	tests.TestExist(t, newDB)
}
