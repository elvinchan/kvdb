package service_test

import (
	"os"
	"testing"
	"time"

	"github.com/elvinchan/kvdb"
	"github.com/elvinchan/kvdb/service"
	"github.com/elvinchan/kvdb/service/server"
)

type MockDB struct {
	store map[string]string
}

func (db *MockDB) Get(key string, opts ...kvdb.GetOption) (*kvdb.Node, error) {
	value, ok := db.store[key]
	if ok {
		return &kvdb.Node{Value: value}, nil
	}
	return nil, nil
}

func (db *MockDB) GetMulti(keys []string, opts ...kvdb.GetOption) (map[string]kvdb.Node, error) {
	v := make(map[string]kvdb.Node, len(keys))
	for _, key := range keys {
		value, ok := db.store[key]
		if ok {
			v[key] = kvdb.Node{Value: value}
		}
	}
	return v, nil
}

func (db *MockDB) Set(key, value string, opts ...kvdb.SetOption) error {
	db.store[key] = value
	return nil
}

func (db *MockDB) SetMulti(kvPairs []string, opts ...kvdb.SetOption) error {
	for i := 0; i < len(kvPairs); i += 2 {
		db.store[kvPairs[i]] = kvPairs[i+1]
	}
	return nil
}

func (db *MockDB) Delete(key string, opts ...kvdb.DeleteOption) error {
	delete(db.store, key)
	return nil
}

func (db *MockDB) DeleteMulti(keys []string, opts ...kvdb.DeleteOption) error {
	for _, key := range keys {
		delete(db.store, key)
	}
	return nil
}

func (db *MockDB) Exist(key string) (bool, error) {
	_, ok := db.store[key]
	return ok, nil
}

func (db *MockDB) Cleanup() error {
	db.store = make(map[string]string)
	return nil
}

func (db *MockDB) Close() error {
	return nil
}

const SockFile = "testdb.sock"

func TestServer(t *testing.T) {
	defer os.Remove(SockFile)
	go func() {
		err := server.StartServer(&MockDB{
			store: make(map[string]string),
		}, "unix", SockFile)
		if err != nil {
			panic(err)
		}
	}()
	time.Sleep(time.Millisecond * 100)
	db, err := service.DialKVDBService("unix", SockFile)
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	key := "service.g"
	value := "0"
	err = db.Set(key, value)
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	rst, err := db.Get(key)
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	if rst == nil {
		t.Errorf("result not right, expect not nil")
		t.Fail()
	} else if rst.Value != value {
		t.Errorf("result not right, expect %s, got %s", value, rst.Value)
		t.Fail()
	}

	has, err := db.Exist(key)
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	if !has {
		t.Errorf("result of Exist() not right, expect %v, got %v", true, has)
		t.Fail()
	}

	err = db.Delete(key)
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	rst, err = db.Get(key)
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	if rst != nil {
		t.Errorf("result not right, expect nil")
		t.Fail()
	}

	kvs := []string{
		"service.g.child1", "1",
		"service.g.child2", "2",
	}
	err = db.SetMulti(kvs)
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	var keys []string
	for i := 0; i < len(kvs); i += 2 {
		keys = append(keys, kvs[i])
	}
	rsts, err := db.GetMulti(keys)
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	if len(rsts) != len(kvs)/2 {
		t.Errorf("length of results not right, expect %v, got %v",
			len(kvs)/2, len(rsts))
		t.Fail()
	}
	for i := 0; i < len(kvs); i += 2 {
		if rsts[kvs[i]].Value != kvs[i+1] {
			t.Errorf("value not right, expect %s, got %s",
				kvs[i+1], rsts[kvs[i]].Value)
			t.Fail()
		}
	}

	err = db.DeleteMulti(keys)
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	rsts, err = db.GetMulti(keys)
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	if len(rsts) != 0 {
		t.Errorf("length of results not right, expect 0")
		t.Fail()
	}

	err = db.Set(key, value)
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	err = db.Cleanup()
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	rst, err = db.Get(key)
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	if rst != nil {
		t.Errorf("result not right, expect nil")
		t.Fail()
	}
}
