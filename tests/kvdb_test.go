package tests

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
}

func TestGetSet(t *testing.T) {
	kvs := []string{
		"group.g", "1",
		"group.g.child1", "2",
		"group.g.child2", "3",
		"group.g.child3", "4",
		"group.g.child3.grandchild1", "5",
		"group.g.child3.grandchild2", "6",
	}
	for driver, url := range testingUrl {
		t.Run(driver, func(t *testing.T) {
			db, err := newDB(driver, url)
			if err != nil {
				panic(err)
			}
			defer func() {
				err = db.Close()
				if err != nil {
					panic(err)
				}
			}()
			rst, err := db.Get(kvs[0])
			if err != nil {
				t.Error(err)
				t.Fail()
			}
			if rst != nil {
				t.Errorf("result not right, expect nil")
				t.Fail()
			}

			rsts, err := db.GetMulti(kvs)
			if err != nil {
				t.Error(err)
				t.Fail()
			}
			if len(rsts) != 0 {
				t.Errorf("length of results not right, expect 0")
				t.Fail()
			}

			value := "z"
			err = db.Set(kvs[0], "z")
			if err != nil {
				t.Error(err)
				t.Fail()
			}

			rst, err = db.Get(kvs[0])
			if err != nil {
				t.Error(err)
				t.Fail()
			}
			if rst == nil {
				t.Errorf("result not right, expect not nil")
				t.Fail()
			} else if rst.Value != value {
				t.Errorf("value not right, expect %s, got %s", value, rst.Value)
				t.Fail()
			} else if rst.Children != nil {
				t.Errorf("children not right, expect nil")
				t.Fail()
			}

			err = db.SetMulti(kvs)
			if err != nil {
				t.Error(err)
				t.Fail()
			}

			for i := 0; i < len(kvs); i += 2 {
				rst, err := db.Get(kvs[i])
				if err != nil {
					t.Error(err)
					t.Fail()
				}
				if rst == nil {
					t.Errorf("result not right, expect not nil")
					t.Fail()
				} else if rst.Value != kvs[i+1] {
					t.Errorf("value not right, expect %s, got %s",
						kvs[i+1], rst.Value)
					t.Fail()
				}
			}

			rsts, err = db.GetMulti(kvs)
			if err != nil {
				t.Error(err)
				t.Fail()
			}
			if rsts == nil {
				t.Errorf("result not right, expect not nil")
				t.FailNow()
			}
			for i := 0; i < len(kvs); i += 2 {
				if rsts[kvs[i]].Value != kvs[i+1] {
					t.Errorf("value not right, expect %s, got %s",
						kvs[i+1], rsts[kvs[i]].Value)
					t.Fail()
				}
			}
		})
	}
}

func TestDelete(t *testing.T) {
	kvs := []string{
		"group.d", "1",
		"group.d.child1", "2",
		"group.d.child2", "3",
		"group.d.child3", "4",
		"group.d.child3.grandchild1", "5",
		"group.d.child3.grandchild2", "6",
	}
	var keys []string
	for i := 0; i < len(kvs); i += 2 {
		keys = append(keys, kvs[i])
	}

	setData := func(driver, url string) kvdb.KVDB {
		db, err := newDB(driver, url)
		if err != nil {
			panic(err)
		}
		err = db.SetMulti(kvs)
		if err != nil {
			t.Error(err)
			t.Fail()
		}
		rsts, err := db.GetMulti(keys)
		if err != nil {
			t.Error(err)
			t.Fail()
		}
		if rsts == nil {
			t.Errorf("value not right, expect not nil")
			t.Fail()
		}
		for i := 0; i < len(kvs); i += 2 {
			if rsts[kvs[i]].Value != kvs[i+1] {
				t.Errorf("value not right, expect %s, got %s",
					kvs[i+1], rsts[kvs[i]].Value)
				t.Fail()
			}
		}
		return db
	}

	for driver, url := range testingUrl {
		t.Run(driver, func(t *testing.T) {
			t.Run("Delete", func(t *testing.T) {
				db := setData(driver, url)
				defer func() {
					err := db.Close()
					if err != nil {
						panic(err)
					}
				}()

				err := db.Delete(kvs[0])
				if err != nil {
					t.Error(err)
					t.Fail()
				}
				for i := 0; i < len(kvs); i += 2 {
					rst, err := db.Get(kvs[i])
					if err != nil {
						t.Error(err)
						t.Fail()
					}
					if i == 0 {
						if rst != nil {
							t.Errorf("result not right, expect nil")
							t.Fail()
						}
					} else {
						if rst == nil {
							t.Errorf("result not right, expect not nil")
							t.Fail()
						} else if rst.Value != kvs[i+1] {
							t.Errorf("value not right, expect %v, got %v", kvs[i+1], rst.Value)
							t.Fail()
						}
					}
				}

				err = db.Delete(kvs[8])
				if err != nil {
					t.Error(err)
					t.Fail()
				}
				for i := 0; i < len(kvs); i = i + 2 {
					rst, err := db.Get(kvs[i])
					if err != nil {
						t.Error(err)
						t.Fail()
					}
					if i == 0 || i == 8 {
						if rst != nil {
							t.Errorf("result not right, expect nil")
							t.Fail()
						}
					} else {
						if rst == nil {
							t.Errorf("result not right, expect not nil")
							t.Fail()
						} else if rst.Value != kvs[i+1] {
							t.Errorf("value not right, expect %v, got %v", kvs[i+1], rst.Value)
							t.Fail()
						}
					}
				}
			})

			t.Run("DeleteWithChildren", func(t *testing.T) {
				db := setData(driver, url)
				defer func() {
					err := db.Close()
					if err != nil {
						panic(err)
					}
				}()

				err := db.Delete(kvs[0], kvdb.DeleteChildren())
				if err != nil {
					t.Error(err)
					t.Fail()
				}
				for i := 0; i < len(kvs); i += 2 {
					rst, err := db.Get(kvs[i])
					if err != nil {
						t.Error(err)
						t.Fail()
					}
					if i/2 < 4 {
						// self and children
						if rst != nil {
							t.Errorf("result not right, expect nil")
							t.Fail()
						}
					} else {
						// grandchildren
						if rst == nil {
							t.Errorf("result not right, expect not nil")
							t.Fail()
						} else if rst.Value != kvs[i+1] {
							t.Errorf("value not right, expect %v, got %v", kvs[i+1], rst.Value)
							t.Fail()
						}
					}
				}
			})

			t.Run("DeleteMulti", func(t *testing.T) {
				db := setData(driver, url)
				defer func() {
					err := db.Close()
					if err != nil {
						panic(err)
					}
				}()

				err := db.DeleteMulti([]string{kvs[0], kvs[8]})
				if err != nil {
					t.Error(err)
					t.Fail()
				}
				for i := 0; i < len(kvs); i = i + 2 {
					rst, err := db.Get(kvs[i])
					if err != nil {
						t.Error(err)
						t.Fail()
					}
					if i == 0 || i == 8 {
						if rst != nil {
							t.Errorf("result not right, expect nil")
							t.Fail()
						}
					} else {
						if rst == nil {
							t.Errorf("result not right, expect not nil")
							t.Fail()
						} else if rst.Value != kvs[i+1] {
							t.Errorf("value not right, expect %v, got %v", kvs[i+1], rst.Value)
							t.Fail()
						}
					}
				}
			})

			t.Run("DeleteMultiWithChildren", func(t *testing.T) {
				db := setData(driver, url)
				defer func() {
					err := db.Close()
					if err != nil {
						panic(err)
					}
				}()

				err := db.DeleteMulti([]string{kvs[0], kvs[8]}, kvdb.DeleteChildren())
				if err != nil {
					t.Error(err)
					t.Fail()
				}
				for i := 0; i < len(kvs); i = i + 2 {
					rst, err := db.Get(kvs[i])
					if err != nil {
						t.Error(err)
						t.Fail()
					}
					if i != 10 {
						if rst != nil {
							t.Errorf("result not right, expect nil")
							t.Fail()
						}
					} else {
						if rst == nil {
							t.Errorf("result not right, expect not nil")
							t.Fail()
						} else if rst.Value != kvs[i+1] {
							t.Errorf("value not right, expect %v, got %v", kvs[i+1], rst.Value)
							t.Fail()
						}
					}
				}
			})
		})
	}
}

func TestExist(t *testing.T) {
	for driver, url := range testingUrl {
		t.Run(driver, func(t *testing.T) {
			db, err := newDB(driver, url)
			if err != nil {
				panic(err)
			}
			defer func() {
				err := db.Close()
				if err != nil {
					panic(err)
				}
			}()
			key := "group.e"
			has, err := db.Exist(key)
			if err != nil {
				t.Error(err)
				t.Fail()
			}

			if has {
				t.Errorf("result of Exist() not right, expect %v, got %v", false, has)
				t.Fail()
			}

			err = db.Set(key, "1")
			if err != nil {
				t.Error(err)
				t.Fail()
			}

			has, err = db.Exist(key)
			if err != nil {
				t.Error(err)
				t.Fail()
			}

			if !has {
				t.Errorf("result of Exist() not right, expect %v, got %v", true, has)
				t.Fail()
			}
		})
	}
}
