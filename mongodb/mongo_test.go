package mongodb

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestCleanup(t *testing.T) {
	db, err := NewDB("mongodb://localhost:27017", "kvdb", "kv")
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

	mdb, ok := db.(*mongoDB)
	if !ok {
		t.Error("failed covert KVDB to MongoDB instance")
		t.Fail()
	}

	keys := []string{"inner.c", "inner.c.child1"}

	now := time.Now()
	for i, key := range keys {
		if i == 1 {
			now = time.Now().Add(time.Minute)
		}
		_, err := mdb.collection.UpdateByID(context.TODO(),
			key,
			bson.D{
				{Key: "$set", Value: bson.D{
					{Key: "v", Value: "test"},
					{Key: "pid", Value: mdb.option.ParentKey(key)},
					{Key: "exp", Value: now},
				}}},
			options.Update().SetUpsert(true),
		)
		if err != nil {
			t.Error(err)
			t.Fail()
		}
		var result bson.M
		err = mdb.collection.FindOne(context.TODO(), bson.D{{
			Key: "_id", Value: key,
		}}).Decode(&result)
		if err != nil {
			t.Error(err)
			t.Fail()
		}
		if result["_id"] != key {
			t.Errorf("key not right, expect %s, got %s", key, result["_id"])
			t.Fail()
		}
	}

	err = mdb.Cleanup()
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	for i, key := range keys {
		var result bson.M
		err = mdb.collection.FindOne(context.TODO(), bson.D{{
			Key: "_id", Value: key,
		}}).Decode(&result)
		if i == 0 {
			if err != mongo.ErrNoDocuments {
				t.Errorf("err not right, expect %s, got %s", mongo.ErrNoDocuments, err)
				t.Fail()
			}
		} else {
			if err != nil {
				t.Error(err)
				t.Fail()
			}
			if result["_id"] != key {
				t.Errorf("key not right, expect %s, got %s", key, result["_id"])
				t.Fail()
			}
		}
	}
}
