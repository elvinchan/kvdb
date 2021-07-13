package rdb

import (
	"testing"
	"time"
)

func TestMaxDatetime(t *testing.T) {
	expect := "9999-12-31 23:59:59"
	str := maxDatetime.Format("2006-01-02 15:04:05")
	if str != expect {
		t.Errorf("max datetime not right, expect %s, got %s", expect, str)
		t.Fail()
	}
}

func TestCleanup(t *testing.T) {
	db, err := NewDB(DriverSqlite3, "sqlite.db?cache=shared")
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

	ormdb, ok := db.(*rdb)
	if !ok {
		t.Error("failed covert KVDB to ORM instance")
		t.Fail()
	}

	keys := []string{"inner.c", "inner.c.child1"}

	now := time.Now()
	for _, key := range keys {
		if err := ormdb.db.Create(&rdbNode{
			Key:       key,
			ParentKey: ormdb.option.ParentKey(key),
			Value:     "test",
			ExpireAt:  now,
		}).Error; err != nil {
			t.Error(err)
			t.Fail()
		}
		var cnt int64
		if err := ormdb.db.Model(rdbNode{}).Where("key = ?", key).
			Count(&cnt).Error; err != nil {
			t.Error(err)
			t.Fail()
		}
		if cnt != 1 {
			t.Errorf("count of key not right, expect %d, got %d", 1, cnt)
			t.Fail()
		}
	}

	if err = db.Cleanup(); err != nil {
		t.Error(err)
		t.Fail()
	}

	for _, key := range keys {
		var cnt int64
		if err := ormdb.db.Model(rdbNode{}).Where("key = ?", key).
			Count(&cnt).Error; err != nil {
			t.Error(err)
			t.Fail()
		}
		if cnt != 0 {
			t.Errorf("count of key not right, expect %d, got %d", 0, cnt)
			t.Fail()
		}
	}
}
