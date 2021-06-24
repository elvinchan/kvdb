package leveldb

import (
	"bytes"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/elvinchan/kvdb"
	"github.com/elvinchan/kvdb/internal"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/vmihailenco/msgpack/v5"
)

type levelDB struct {
	db     *leveldb.DB
	option *internal.Option
}

func NewDB(path string, opts ...internal.DBOption) (kvdb.KVDB, error) {
	o := internal.InitOption()
	for _, opt := range opts {
		opt(o)
	}
	db, err := leveldb.OpenFile(path, nil)
	return &levelDB{
		db:     db,
		option: o,
	}, err
}

type levelDBNode struct {
	Value    string    `msgpack:"value"`
	ExpireAt time.Time `msgpack:"expire_at,omitempty"`
}

func (l *levelDB) Get(key string, opts ...internal.GetOption,
) (*kvdb.Node, error) {
	var gt internal.Getter
	for _, opt := range opts {
		opt(&gt)
	}
	if gt.Children && gt.Limit == 0 {
		gt.Limit = l.option.DefaultLimit
	}
	node, deleteKeys, err := l.get(key, gt)
	if err != nil {
		return nil, err
	}
	if len(deleteKeys) > 0 {
		err = l.DeleteMulti(deleteKeys)
	}
	return node, err
}

func (l *levelDB) GetMulti(keys []string, opts ...internal.GetOption,
) (map[string]kvdb.Node, error) {
	var gt internal.Getter
	for _, opt := range opts {
		opt(&gt)
	}
	if gt.Children && gt.Limit == 0 {
		gt.Limit = l.option.DefaultLimit
	}
	var (
		v          map[string]kvdb.Node
		deleteKeys []string
	)
	for i := range keys {
		node, dks, err := l.get(keys[i], gt)
		if err != nil {
			return nil, err
		}
		v[keys[i]] = *node
		deleteKeys = append(deleteKeys, dks...)
	}
	if len(deleteKeys) == 0 {
		return v, nil
	}
	return v, l.DeleteMulti(deleteKeys)
}

func (l *levelDB) get(key string, gt internal.Getter,
) (*kvdb.Node, []string, error) {
	v, err := l.db.Get(l.mask(key), nil)
	if err != nil {
		if errors.Is(err, leveldb.ErrNotFound) {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	var deleteKeys []string
	now := time.Now()
	retrive := func(key string, data []byte) (string, error) {
		n, err := l.decode(data)
		if err != nil {
			return "", err
		}
		if !n.ExpireAt.IsZero() && !n.ExpireAt.After(now) {
			if l.option.AutoClean {
				deleteKeys = append(deleteKeys, key)
			}
			return "", nil
		}
		return n.Value, nil
	}
	var node kvdb.Node
	node.Value, err = retrive(key, v)
	if err != nil {
		return nil, nil, err
	}

	bareStartKey := l.option.IsBareKey(gt.Start)
	parentStartKey := l.option.ParentBareKey(gt.Start)
	if gt.Limit > 0 && (bareStartKey || parentStartKey == key) {
		node.Children = make(map[string]string, gt.Limit)
		iter := l.db.NewIterator(
			l.leveldbRange(key, l.option.BareKey(gt.Start)),
			nil,
		)
		defer iter.Release()
		var i int
		for iter.Next() {
			k := l.unmask(iter.Key())
			v, err := retrive(k, iter.Value())
			if err != nil {
				return nil, nil, err
			}
			if v == "" {
				continue
			}
			node.Children[k] = v
			i++
			if i >= gt.Limit {
				break
			}
		}
	}
	return &node, deleteKeys, nil
}

func (l *levelDB) leveldbRange(key, start string) *util.Range {
	prefix := l.mask(key + l.option.KeyPathSep + start)
	var limit []byte
	for i := len(prefix) - 1; i >= 0; i-- {
		c := prefix[i]
		if c < 0xff {
			limit = make([]byte, i+1)
			copy(limit, prefix)
			limit[i] = c + 1
			break
		}
	}
	return &util.Range{Start: nil, Limit: nil}
}

func (l *levelDB) Set(key, value string, opts ...internal.SetOption) error {
	var st internal.Setter
	for _, opt := range opts {
		opt(&st)
	}
	v, err := l.encode(value, st.ExpireAt)
	if err != nil {
		return err
	}
	return l.db.Put(l.mask(key), v, nil)
}

func (l *levelDB) SetMulti(kvPairs []string, opts ...internal.SetOption) error {
	var st internal.Setter
	for _, opt := range opts {
		opt(&st)
	}
	if len(kvPairs)%2 != 0 {
		return errors.New("invalid key value pairs count")
	}
	batch := new(leveldb.Batch)
	for i := 0; i < len(kvPairs)/2; i++ {
		v, err := l.encode(kvPairs[i*2+1], st.ExpireAt)
		if err != nil {
			return err
		}
		batch.Put(l.mask(kvPairs[i*2]), v)
	}
	return l.db.Write(batch, nil)
}

func (l *levelDB) Exist(key string) (bool, error) {
	return l.db.Has(l.mask(key), nil)
}

func (l *levelDB) Delete(key string, opts ...internal.DeleteOption) error {
	var dt internal.Deleter
	for _, opt := range opts {
		opt(&dt)
	}
	if dt.Children {
		batch := new(leveldb.Batch)
		batch.Delete(l.mask(key))
		iter := l.db.NewIterator(l.leveldbRange(key, ""), nil)
		for iter.Next() {
			batch.Delete(iter.Key())
		}
		iter.Release()
		return l.db.Write(batch, nil)
	}
	return l.db.Delete(l.mask(key), nil)
}

func (l *levelDB) DeleteMulti(keys []string, opts ...internal.DeleteOption) error {
	var dt internal.Deleter
	for _, opt := range opts {
		opt(&dt)
	}
	if len(keys) == 1 {
		return l.Delete(keys[0], opts...)
	}
	batch := new(leveldb.Batch)
	for _, key := range keys {
		batch.Delete(l.mask(key))
		if !dt.Children {
			continue
		}
		iter := l.db.NewIterator(l.leveldbRange(key, ""), nil)
		for iter.Next() {
			batch.Delete(iter.Key())
		}
		iter.Release()
	}
	return l.db.Write(batch, nil)
}

func (l *levelDB) Cleanup() error {
	batch := new(leveldb.Batch)
	now := time.Now()
	iter := l.db.NewIterator(nil, nil)
	defer iter.Release()
	for iter.Next() {
		node, err := l.decode(iter.Value())
		if err != nil {
			return err
		}
		if node.ExpireAt.IsZero() || node.ExpireAt.After(now) {
			continue
		}
		batch.Delete(iter.Key())
	}
	return l.db.Write(batch, nil)
}

func (l *levelDB) Close() error {
	return l.db.Close()
}

func (levelDB) encode(value string, expireAt time.Time) ([]byte, error) {
	node := levelDBNode{
		Value:    value,
		ExpireAt: expireAt,
	}
	return msgpack.Marshal(&node)
}

func (levelDB) decode(data []byte) (*levelDBNode, error) {
	var node levelDBNode
	err := msgpack.Unmarshal(data, &node)
	return &node, err
}

func (l *levelDB) mask(key string) []byte {
	var buffer bytes.Buffer
	buffer.WriteString("node:")
	buffer.WriteString(strconv.Itoa(strings.Count(key, l.option.KeyPathSep)))
	buffer.WriteString(":")
	buffer.WriteString(key)
	return buffer.Bytes()
}

func (levelDB) unmask(key []byte) string {
	idx := bytes.LastIndex(key, []byte(":"))
	if idx == -1 {
		return string(key)
	}
	return string(key[idx+1:])
}
