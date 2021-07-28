package leveldb

import (
	"testing"
	"time"
)

func TestCleanup(t *testing.T) {
	db, err := NewDB("leveldb.db")
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	defer func() {
		err := db.Close()
		if err != nil {
			panic(err)
		}
	}()

	ldb, ok := db.(*levelDB)
	if !ok {
		t.Error("failed covert KVDB to LevelDB instance")
		t.Fail()
	}

	keys := []string{"inner.c", "inner.c.child1"}

	now := time.Now()
	for i, key := range keys {
		if i == 1 {
			now = time.Now().Add(time.Minute)
		}
		v, err := ldb.encode("test", now)
		if err != nil {
			t.Error(err)
			t.Fail()
		}
		err = ldb.db.Put(ldb.mask(key), v, nil)
		if err != nil {
			t.Error(err)
			t.Fail()
		}

		has, err := ldb.db.Has(ldb.mask(key), nil)
		if err != nil {
			t.Error(err)
			t.Fail()
		}
		if !has {
			t.Errorf("key not exist")
			t.Fail()
		}
	}

	err = ldb.Cleanup()
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	for i, key := range keys {
		has, err := ldb.db.Has(ldb.mask(key), nil)
		if err != nil {
			t.Error(err)
			t.Fail()
		}
		if has && i == 0 {
			t.Errorf("key %s exist", key)
			t.Fail()
		} else if !has && i == 1 {
			t.Errorf("key %s not exist", key)
			t.Fail()
		}
	}
}
