package leveldb

import (
	"bytes"
	"errors"
	"log"
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
	db      *leveldb.DB
	option  *kvdb.Option
	loadRec *internal.LoadRec
	close   chan struct{}
}

func NewDB(path string, opts ...kvdb.DBOption) (kvdb.KVDB, error) {
	o := kvdb.InitOption()
	for _, opt := range opts {
		opt(o)
	}
	db, err := leveldb.OpenFile(path, nil)
	v := levelDB{
		db:      db,
		option:  o,
		loadRec: internal.DefaultLoadRec(),
		close:   make(chan struct{}),
	}
	if o.AutoClean {
		go v.loadRec.StartClean(func() {
			if err := v.Cleanup(); err != nil {
				log.Println("cleanup error when auto clean", err)
			}
		}, v.close)
	}
	return &v, err
}

type levelDBNode struct {
	Value    string    `msgpack:"value"`
	ExpireAt time.Time `msgpack:"expire_at,omitempty"`
}

func (l *levelDB) Get(key string, opts ...kvdb.GetOption,
) (*kvdb.Node, error) {
	var gt kvdb.Getter
	for _, opt := range opts {
		opt(&gt)
	}
	if gt.Children && gt.Limit == 0 {
		gt.Limit = l.option.DefaultLimit
	}
	now := time.Now()
	defer l.hookReq(time.Since(now))
	node, deleteKeys, err := l.get(key, &gt)
	if err != nil {
		return nil, err
	}
	if len(deleteKeys) > 0 {
		err = l.deleteMulti(deleteKeys, nil)
	}
	return node, err
}

func (l *levelDB) GetMulti(keys []string, opts ...kvdb.GetOption,
) (map[string]kvdb.Node, error) {
	var gt kvdb.Getter
	for _, opt := range opts {
		opt(&gt)
	}
	if gt.Children && gt.Limit == 0 {
		gt.Limit = l.option.DefaultLimit
	}
	now := time.Now()
	defer l.hookReq(time.Since(now))
	var (
		v          = make(map[string]kvdb.Node, len(keys))
		deleteKeys []string
	)
	for i := range keys {
		node, dks, err := l.get(keys[i], &gt)
		if err != nil {
			return nil, err
		}
		if node != nil {
			v[keys[i]] = *node
		}
		deleteKeys = append(deleteKeys, dks...)
	}
	var err error
	if len(deleteKeys) > 0 {
		err = l.deleteMulti(deleteKeys, nil)
	}
	return v, err
}

func (l *levelDB) get(key string, gt *kvdb.Getter,
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

	isBareStartKey := l.option.IsBareKey(gt.Start)
	parentStartKey := l.option.ParentKey(gt.Start)
	if gt.Children && (isBareStartKey || parentStartKey == key) {
		node.Children = make(map[string]string, gt.Limit)
		iter := l.db.NewIterator(
			l.childRange(key, l.option.BareKey(gt.Start)),
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
			if gt.Limit > 0 && i >= gt.Limit {
				break
			}
		}
	}
	return &node, deleteKeys, nil
}

func nextBytes(key []byte) []byte {
	for i := len(key) - 1; i >= 0; i-- {
		if key[i] < 0xff {
			key[i] = key[i] + 1
			break
		}
	}
	return key
}

// childRange generate LevelDB's Range for iterator
func (l *levelDB) childRange(parentKey, startBareKey string) *util.Range {
	startBare := []byte(startBareKey)
	if startBareKey != "" {
		startBare = nextBytes(startBare)
	}
	parent := []byte(parentKey)
	start := l.maskBytes(parent, startBare)
	limit := l.maskBytes(nextBytes(parent), []byte{})
	return &util.Range{Start: start, Limit: limit}
}

func (l *levelDB) Set(key, value string, opts ...kvdb.SetOption) error {
	var st kvdb.Setter
	for _, opt := range opts {
		opt(&st)
	}
	now := time.Now()
	defer l.hookReq(time.Since(now))
	v, err := l.encode(value, st.ExpireAt)
	if err != nil {
		return err
	}
	return l.db.Put(l.mask(key), v, nil)
}

func (l *levelDB) SetMulti(kvPairs []string, opts ...kvdb.SetOption) error {
	var st kvdb.Setter
	for _, opt := range opts {
		opt(&st)
	}
	if len(kvPairs)%2 != 0 {
		return errors.New("invalid key value pairs count")
	}
	now := time.Now()
	defer l.hookReq(time.Since(now))
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
	now := time.Now()
	defer l.hookReq(time.Since(now))
	return l.db.Has(l.mask(key), nil)
}

func (l *levelDB) Delete(key string, opts ...kvdb.DeleteOption) error {
	var dt kvdb.Deleter
	for _, opt := range opts {
		opt(&dt)
	}
	now := time.Now()
	defer l.hookReq(time.Since(now))
	return l.delete(key, &dt)
}

func (l *levelDB) delete(key string, dt *kvdb.Deleter) error {
	if dt != nil && dt.Children {
		batch := new(leveldb.Batch)
		batch.Delete(l.mask(key))
		iter := l.db.NewIterator(l.childRange(key, ""), nil)
		for iter.Next() {
			batch.Delete(iter.Key())
		}
		iter.Release()
		return l.db.Write(batch, nil)
	}
	return l.db.Delete(l.mask(key), nil)
}

func (l *levelDB) DeleteMulti(keys []string, opts ...kvdb.DeleteOption) error {
	var dt kvdb.Deleter
	for _, opt := range opts {
		opt(&dt)
	}
	if len(keys) == 0 {
		return nil
	}
	now := time.Now()
	defer l.hookReq(time.Since(now))
	if len(keys) == 1 {
		return l.delete(keys[0], &dt)
	}
	return l.deleteMulti(keys, &dt)
}

func (l *levelDB) deleteMulti(keys []string, dt *kvdb.Deleter) error {
	batch := new(leveldb.Batch)
	for _, key := range keys {
		batch.Delete(l.mask(key))
		if dt == nil || !dt.Children {
			continue
		}
		iter := l.db.NewIterator(l.childRange(key, ""), nil)
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
	close(l.close)
	return l.db.Close()
}

func (l *levelDB) hookReq(score time.Duration) {
	if l.option.AutoClean {
		l.loadRec.HookReq(int64(score))
	}
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
	buffer.WriteString(strconv.Itoa(strings.Count(key, l.option.KeyPathSep) + 1))
	buffer.WriteString(":")
	buffer.WriteString(key)
	return buffer.Bytes()
}

func (l *levelDB) maskBytes(keyParts ...[]byte) []byte {
	var level int
	for _, kp := range keyParts {
		level += bytes.Count(kp, []byte(l.option.KeyPathSep)) + 1
	}
	var buffer bytes.Buffer
	buffer.WriteString("node:")
	buffer.WriteString(strconv.Itoa(level))
	buffer.WriteString(":")
	for _, kp := range keyParts {
		buffer.Write(kp)
		buffer.WriteString(l.option.KeyPathSep)
	}
	return buffer.Bytes()[:buffer.Len()-1]
}

func (levelDB) unmask(key []byte) string {
	idx := bytes.LastIndex(key, []byte(":"))
	if idx == -1 {
		return string(key)
	}
	return string(key[idx+1:])
}
