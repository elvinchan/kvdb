package kvdb

import (
	"strings"
	"time"
)

const (
	KeyPathSep   = "."
	DefaultLimit = 10
)

type Node struct {
	Value    string            `json:"value"`
	Children map[string]string `json:"children,omitempty"`
	// ExpireAt time.Time         `json:"-"` // unix seconds
}

type Getter struct {
	Start string // 可以是full key, 也可以是bare key， bare key 支持每个key查询对应的children
	Limit int
}

type GetOption func(g *Getter)

func GetChildren(start string, limit int) GetOption {
	return func(g *Getter) {
		if limit <= 0 {
			limit = DefaultLimit
		}
		g.Start = start
		g.Limit = limit
	}
}

type Setter struct {
	ExpireAt time.Time
}

type SetOption func(s *Setter)

func SetExpire(at time.Time) SetOption {
	return func(s *Setter) {
		s.ExpireAt = at
	}
}

type Deleter struct {
	Children bool
}

type DeleteOption func(d *Deleter)

func DeleteChildren(b bool) DeleteOption {
	return func(d *Deleter) {
		d.Children = true
	}
}

type KVDB interface {
	// ex: key: a.b.c
	Get(key string, opts ...GetOption) (*Node, error)

	GetMulti(keys []string, opts ...GetOption) ([]Node, error)

	Set(key, value string, opts ...SetOption) error

	SetMulti(kvPairs []string, opts ...SetOption) error

	// DEL node:key SREM children:key
	Delete(key string, opts ...DeleteOption) error

	DeleteMulti(keys []string, opts ...DeleteOption) error

	Exist(key string) (bool, error)

	Close() error
}

func ParseParentKey(key string) string {
	segs := strings.Split(key, KeyPathSep)
	if len(segs) > 1 {
		return segs[len(segs)-2]
	}
	return ""
}

func BareKey(key string) string {
	idx := strings.LastIndex(key, KeyPathSep)
	if idx == -1 {
		return ""
	}
	return key[idx:]
}

func IsBareKey(key string) bool {
	return !strings.Contains(key, KeyPathSep)
}

func FullKey(bareKey, parentKey string) string {
	return parentKey + KeyPathSep + bareKey
}

type DB struct {
	AutoClean bool
	Debug     bool
}

type DBOption func(d *DB)

func AutoClean() DBOption {
	return func(d *DB) {
		d.AutoClean = true
	}
}

func Debug() DBOption {
	return func(d *DB) {
		d.Debug = true
	}
}
