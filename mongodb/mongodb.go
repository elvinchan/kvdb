package mongodb

import (
	"context"
	"errors"
	"time"

	"github.com/elvinchan/kvdb"
	"github.com/elvinchan/kvdb/internal"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

var maxDatetime, _ = time.Parse("2006-01-02 15:04:05", "9999-12-31 23:59:59")

type mongoDB struct {
	option     *kvdb.Option
	loadRec    *internal.LoadRec
	collection *mongo.Collection
}

// NewDB create a KVDB instance with mongo client
func NewDB(uri, database, collection string,
	opts ...kvdb.DBOption) (kvdb.KVDB, error) {
	o := kvdb.InitOption()
	for _, opt := range opts {
		opt(o)
	}
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}
	if err = client.Ping(context.TODO(), readpref.Primary()); err != nil {
		return nil, err
	}
	v := mongoDB{
		option:     o,
		loadRec:    internal.DefaultLoadRec(),
		collection: client.Database(database).Collection(collection),
	}
	_, err = v.collection.Indexes().CreateMany(
		context.TODO(), []mongo.IndexModel{
			{Keys: bson.D{
				{Key: "exp", Value: -1},
			}},
			{Keys: bson.D{
				{Key: "pid", Value: 1},
			}},
		})
	return &v, err
}

func (m *mongoDB) Get(key string, opts ...kvdb.GetOption,
) (*kvdb.Node, error) {
	var gt kvdb.Getter
	for _, opt := range opts {
		opt(&gt)
	}
	if gt.Children && gt.Limit == 0 {
		gt.Limit = m.option.DefaultLimit
	}
	now := time.Now()
	defer m.hookReq(time.Since(now))
	var result bson.M
	err := m.collection.FindOne(
		context.TODO(), bson.D{
			{Key: "_id", Value: key},
			{Key: "exp", Value: bson.D{
				{Key: "$gt", Value: now},
			}},
		},
	).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, err
	}
	var v kvdb.Node
	v.Value = result["v"].(string)
	if gt.Children {
		if err = m.getChildren(key, &v, now, &gt); err != nil {
			return nil, err
		}
	}
	return &v, nil
}

func (m *mongoDB) GetMulti(keys []string, opts ...kvdb.GetOption,
) (map[string]kvdb.Node, error) {
	var gt kvdb.Getter
	for _, opt := range opts {
		opt(&gt)
	}
	if gt.Children && gt.Limit == 0 {
		gt.Limit = m.option.DefaultLimit
	}
	now := time.Now()
	defer m.hookReq(time.Since(now))
	cur, err := m.collection.Find(
		context.TODO(), bson.D{
			{Key: "_id", Value: bson.D{
				{Key: "$in", Value: keys},
			}},
			{Key: "exp", Value: bson.D{
				{Key: "$gt", Value: now},
			}},
		},
	)
	if err != nil {
		return nil, err
	}

	var results []bson.M
	if err := cur.All(context.TODO(), &results); err != nil {
		return nil, err
	}
	isBareStartKey := m.option.IsBareKey(gt.Start)
	parentStartKey := m.option.ParentKey(gt.Start)
	v := make(map[string]kvdb.Node, len(results))
	for _, result := range results {
		node := kvdb.Node{
			Value: result["v"].(string),
		}
		k := result["_id"].(string)
		if gt.Children && (isBareStartKey || parentStartKey == k) {
			if err := m.getChildren(k, &node, now, &gt); err != nil {
				return nil, err
			}
		}
		v[k] = node
	}
	return v, nil
}

func (m *mongoDB) getChildren(
	k string, v *kvdb.Node, now time.Time, gt *kvdb.Getter) error {
	var opt *options.FindOptions
	if gt.Limit > 0 {
		opt = options.Find().SetLimit(int64(gt.Limit))
	}
	cur, err := m.collection.Find(
		context.TODO(), bson.D{
			{Key: "pid", Value: k},
			{Key: "exp", Value: bson.D{
				{Key: "$gt", Value: now},
			}},
			{Key: "_id", Value: bson.D{
				{Key: "$gt", Value: m.option.FullKey(
					m.option.BareKey(gt.Start), k,
				)},
			}},
		}, opt,
	)
	if err != nil {
		return err
	}
	var childs []bson.M
	if err := cur.All(context.TODO(), &childs); err != nil {
		return err
	}
	v.Children = make(map[string]string, len(childs))
	for _, child := range childs {
		v.Children[child["_id"].(string)] = child["v"].(string)
	}
	return nil
}

func (m *mongoDB) Set(key, value string, opts ...kvdb.SetOption) error {
	var st kvdb.Setter
	for _, opt := range opts {
		opt(&st)
	}
	if st.ExpireAt.IsZero() {
		st.ExpireAt = maxDatetime
	}
	_, err := m.collection.UpdateByID(context.TODO(),
		key,
		bson.D{
			{Key: "$set", Value: bson.D{
				{Key: "v", Value: value},
				{Key: "pid", Value: m.option.ParentKey(key)},
				{Key: "exp", Value: st.ExpireAt},
			}}},
		options.Update().SetUpsert(true),
	)
	return err
}

func (m *mongoDB) SetMulti(kvPairs []string, opts ...kvdb.SetOption) error {
	var st kvdb.Setter
	for _, opt := range opts {
		opt(&st)
	}
	if st.ExpireAt.IsZero() {
		st.ExpireAt = maxDatetime
	}
	if len(kvPairs)%2 != 0 {
		return errors.New("invalid key value pairs count")
	}
	now := time.Now()
	defer m.hookReq(time.Since(now))
	var oprs []mongo.WriteModel
	for i := 0; i < len(kvPairs)/2; i++ {
		opr := mongo.NewUpdateOneModel()
		opr.SetFilter(bson.D{
			{Key: "_id", Value: kvPairs[i*2]},
		})
		opr.SetUpdate(bson.D{{Key: "$set", Value: bson.D{
			{Key: "v", Value: kvPairs[i*2+1]},
			{Key: "pid", Value: m.option.ParentKey(kvPairs[i*2])},
			{Key: "exp", Value: st.ExpireAt},
		}}})
		opr.SetUpsert(true)
		oprs = append(oprs, opr)
	}
	_, err := m.collection.BulkWrite(context.TODO(), oprs)
	return err
}

func (m *mongoDB) Exist(key string) (bool, error) {
	var result bson.M
	err := m.collection.FindOne(context.TODO(), bson.D{{
		Key: "_id", Value: key,
	}}).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (m *mongoDB) Delete(key string, opts ...kvdb.DeleteOption) error {
	var dt kvdb.Deleter
	for _, opt := range opts {
		opt(&dt)
	}
	filter := bson.D{
		{Key: "_id", Value: key},
	}
	if dt.Children {
		filter = bson.D{
			{Key: "$or", Value: bson.A{
				filter,
				bson.D{{Key: "pid", Value: key}},
			}},
		}
	}
	_, err := m.collection.DeleteMany(context.TODO(), filter)
	return err
}

func (m *mongoDB) DeleteMulti(keys []string, opts ...kvdb.DeleteOption) error {
	var dt kvdb.Deleter
	for _, opt := range opts {
		opt(&dt)
	}
	var ids bson.A
	for _, key := range keys {
		ids = append(ids, key)
	}
	filter := bson.D{
		{Key: "_id", Value: bson.D{
			{Key: "$in", Value: ids},
		}},
	}
	if dt.Children {
		filter = bson.D{
			{Key: "$or", Value: bson.A{
				filter,
				bson.D{
					{Key: "pid", Value: bson.D{
						{Key: "$in", Value: ids},
					}},
				},
			}},
		}
	}
	_, err := m.collection.DeleteMany(context.TODO(), filter)
	return err
}

func (m *mongoDB) Cleanup() error {
	_, err := m.collection.DeleteMany(context.TODO(), bson.D{
		{Key: "exp", Value: bson.D{
			{Key: "$lte", Value: time.Now()},
		}},
	})
	return err
}

func (m *mongoDB) Close() error {
	return m.collection.Database().Client().Disconnect(context.TODO())
}

func (m *mongoDB) hookReq(score time.Duration) {
	if m.option.AutoClean {
		m.loadRec.HookReq(int64(score))
	}
}
