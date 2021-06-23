package kvdb

import (
	"time"

	"github.com/elvinchan/kvdb/internal"
)

type Node struct {
	Value    string            `json:"value"`
	Children map[string]string `json:"children,omitempty"`
	// ExpireAt time.Time         `json:"-"` // unix seconds
}

type GetOption func(g *internal.Getter)

func GetChildren(start string, limit int) GetOption {
	return func(g *internal.Getter) {
		g.Children = true
		g.Start = start
		g.Limit = limit
	}
}

type SetOption func(s *internal.Setter)

func SetExpire(at time.Time) SetOption {
	return func(s *internal.Setter) {
		s.ExpireAt = at
	}
}

type DeleteOption func(d *internal.Deleter)

func DeleteChildren(b bool) DeleteOption {
	return func(d *internal.Deleter) {
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

type DBOption func(d *internal.Option)

func AutoClean() DBOption {
	return func(d *internal.Option) {
		d.AutoClean = true
	}
}

func Debug() DBOption {
	return func(d *internal.Option) {
		d.Debug = true
	}
}

func KeyPathSep(s string) DBOption {
	return func(d *internal.Option) {
		d.KeyPathSep = s
	}
}

func DefaultLimit(l int) DBOption {
	return func(d *internal.Option) {
		d.DefaultLimit = l
	}
}
