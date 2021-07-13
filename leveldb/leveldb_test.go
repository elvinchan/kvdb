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
		t.Error("failed covert KVDB to ORM instance")
		t.Fail()
	}

	keys := []string{"inner.c", "inner.c.child1"}

	now := time.Now()
	for _, key := range keys {
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

	for _, key := range keys {
		has, err := ldb.db.Has(ldb.mask(key), nil)
		if err != nil {
			t.Error(err)
			t.Fail()
		}
		if has {
			t.Errorf("key exist")
			t.Fail()
		}
	}
}
