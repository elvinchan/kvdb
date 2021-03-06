package rdb_test

import (
	"testing"

	"github.com/elvinchan/kvdb"
	"github.com/elvinchan/kvdb/rdb"
	"github.com/elvinchan/kvdb/tests"
)

func newDB() (kvdb.KVDB, error) {
	return rdb.NewDB(rdb.DriverSqlite3, url+"?cache=shared",
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
		_, err := rdb.NewDB(-1, "")
		if err != rdb.UnsupportDriver {
			t.Errorf("err not right, expect %s, got %s", rdb.UnsupportDriver, err)
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
