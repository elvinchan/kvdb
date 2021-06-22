package test

import (
	"testing"

	"github.com/elvinchan/kvdb"
	"github.com/elvinchan/kvdb/leveldb"
	"github.com/elvinchan/kvdb/rdb"
)

var testingUrl = map[string]string{
	"LevelDB": "leveldb.db",
	"Sqlite3": "sqlite.db",
}

func newDB(driver, url string) (kvdb.KVDB, error) {
	if driver == "LevelDB" {
		return leveldb.NewDB(url)
	} else if driver == "Sqlite3" {
		return rdb.NewDB(rdb.DriverSqlite3, url)
	}
	return nil, nil
}

func TestNewDB(t *testing.T) {
	for driver, url := range testingUrl {
		t.Run(driver, func(t *testing.T) {
			db, err := newDB(driver, url)
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
	}

	// // abnormal
	// t.Run("Abnormal", func(t *testing.T) {
	// 	_, err := kvdb.NewDB("memory://memory.db")
	// 	if err != kvdb.UnsupportDriver {
	// 		t.Errorf("value not right, expect %s, got %s", kvdb.UnsupportDriver, err)
	// 		t.Fail()
	// 	}
	// })
}

func TestGetSet(t *testing.T) {
	for driver, url := range testingUrl {
		t.Run(driver, func(t *testing.T) {
			db, err := newDB(driver, url)
			if err != nil {
				panic(err)
			}
			v := "2"
			err = db.Set("group", v)
			if err != nil {
				t.Error(err)
				t.Fail()
			}

			rst, err := db.Get("group")
			if err != nil {
				t.Error(err)
				t.Fail()
			}

			if rst == nil {
				t.Errorf("value is nil")
				t.Fail()
			} else if rst.Value != v {
				t.Errorf("value not right, expect %s, got %s", v, rst.Value)
				t.Fail()
			}

			err = db.SetMulti([]string{
				"group", v,
				"group.kid1", "3",
				"group.kid2", "4",
			})
			if err != nil {
				t.Error(err)
				t.Fail()
			}

			node, err := db.Get("group", kvdb.GetChildren("", 0))
			if err != nil {
				t.Error(err)
				t.Fail()
			}

			if node.Children["group.kid1"] != "3" {
				t.Errorf("value not right, expect %s, got %s", "3", node.Children["group.kid1"])
				t.Fail()
			}
			if node.Children["group.kid2"] != "4" {
				t.Errorf("value not right, expect %s, got %s", "4", node.Children["group.kid2"])
				t.Fail()
			}

			err = db.Delete("group", kvdb.DeleteChildren(true))
			if err != nil {
				t.Error(err)
				t.Fail()
			}

			rst, err = db.Get("group")
			if err != nil {
				t.Error(err)
				t.Fail()
			}
			if rst != nil {
				t.Errorf("value not right, expect %v, got %v", nil, rst)
				t.Fail()
			}

			rst, err = db.Get("group.kid1")
			if err != nil {
				t.Error(err)
				t.Fail()
			}
			if rst != nil {
				t.Errorf("value not right, expect %v, got %v", nil, rst)
				t.Fail()
			}

			err = db.Close()
			if err != nil {
				t.Error(err)
				t.Fail()
			}
		})
	}
}
