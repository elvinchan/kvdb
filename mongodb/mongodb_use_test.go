package mongodb_test

import (
	"testing"

	"github.com/elvinchan/kvdb"
	"github.com/elvinchan/kvdb/mongodb"
	"github.com/elvinchan/kvdb/tests"
)

func newDB() (kvdb.KVDB, error) {
	return mongodb.NewDB(uri, database, collection,
		kvdb.DefaultLimit(tests.DefaultLimit))
}

func TestNewDB(t *testing.T) {
	t.Run("Normal", func(t *testing.T) {
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
	})

	t.Run("Abnormal", func(t *testing.T) {
		_, err := mongodb.NewDB("", "", "")
		if err == nil {
			t.Error("err not right, expect not nil")
			t.Fail()
		}
	})
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
