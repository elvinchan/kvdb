package internal

import (
	"strings"
	"time"
)

type Option struct {
	AutoClean    bool
	KeyPathSep   string
	DefaultLimit int
	Debug        bool
}

type DBOption func(d *Option)

func InitOption() *Option {
	return &Option{
		KeyPathSep:   ".",
		DefaultLimit: 10,
	}
}

func (db *Option) ParentKey(key string) string {
	idx := strings.LastIndex(key, db.KeyPathSep)
	if idx == -1 {
		return ""
	}
	return key[:idx]
}

func (db *Option) ParentBareKey(key string) string {
	segs := strings.Split(key, db.KeyPathSep)
	if len(segs) > 1 {
		return segs[len(segs)-2]
	}
	return ""
}

func (db *Option) BareKey(key string) string {
	idx := strings.LastIndex(key, db.KeyPathSep)
	if idx == -1 {
		return key
	}
	return key[idx+1:]
}

func (db *Option) IsBareKey(key string) bool {
	return !strings.Contains(key, db.KeyPathSep)
}

func (db *Option) FullKey(bareKey, parentKey string) string {
	if parentKey == "" {
		return bareKey
	}
	return strings.Join([]string{parentKey, bareKey}, db.KeyPathSep)
}

type Getter struct {
	Children bool
	Start    string
	Limit    int
}

type GetOption func(g *Getter)

type Setter struct {
	ExpireAt time.Time
}

type SetOption func(s *Setter)

type Deleter struct {
	Children bool
}

type DeleteOption func(d *Deleter)
