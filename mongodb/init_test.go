package mongodb_test

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	uri        = "mongodb://localhost:27017"
	database   = "kvdb"
	collection = "kv"
)

func init() {
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(uri))
	if err != nil {
		panic(err)
	}
	_, err = client.Database(database).
		Collection(collection).
		DeleteMany(context.TODO(), bson.D{{}})
	if err != nil {
		panic(err)
	}
}
