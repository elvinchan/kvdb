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

// GetChildren specify to get children and set children pagination for `Get()`
// or `GetMulti()`.
// Start is the start key of children, could be full key or bare key, if using
// for `GetMulti()` to get children of every key, should use bare key for start.
// Limit is the maximum count of children of every key.
func GetChildren(start string, limit int) internal.GetOption {
	return func(g *internal.Getter) {
		g.Children = true
		g.Start = start
		g.Limit = limit
	}
}

// SetExpire set expire time of key(s) for `Set()` or `SetMulti()`.
// Expired key value data is not deleted immediately after expire, the actual
// delete timing depends on the logic of auto clean or manually call `Clean()`.
func SetExpire(at time.Time) internal.SetOption {
	return func(s *internal.Setter) {
		s.ExpireAt = at
	}
}

// DeleteChildren specify to delete children for `Delete()` or `DeleteMulti()`.
func DeleteChildren() internal.DeleteOption {
	return func(d *internal.Deleter) {
		d.Children = true
	}
}

type KVDB interface {
	// Get get node of key, which include value and optional children key value
	// pairs with pagination.
	Get(key string, opts ...internal.GetOption) (*Node, error)

	// GetMulti get node map of keys, which include value and optional children
	// key value pairs with pagination of every key.
	GetMulti(keys []string, opts ...internal.GetOption) (map[string]Node, error)

	// Set set value for key with options, which you can specify expire time of
	// key.
	Set(key, value string, opts ...internal.SetOption) error

	// SetMulti set key value pairs with options, which you can specify expire
	// time of keys.
	// For example, SetMulti([]string{"a", 1, "b", 2}) means set value 1 for key
	// a and set value 2 for key b.
	SetMulti(kvPairs []string, opts ...internal.SetOption) error

	// Delete delete key with options, which you can specify also delete
	// children of this key.
	// Delete would not effect on any other keys, for example, if you delete the
	// key without any option, you can still use it's child keys or parent key.
	Delete(key string, opts ...internal.DeleteOption) error

	// DeleteMulti delete keys with options, which you can specify also delete
	// children of these keys.
	// DeleteMulti would not effect on any other keys, for example, if you
	// delete the key without any option, you can still use it's child keys or
	// parent key.
	DeleteMulti(keys []string, opts ...internal.DeleteOption) error

	// Exist check if key is exist.
	Exist(key string) (bool, error)

	// Cleanup delete all expired keys from DB.
	Cleanup() error

	// Close close DB. should only execute once and cannot use after close.
	Close() error
}

// AutoClean specify to enable auto clean process of DB.
func AutoClean() internal.DBOption {
	return func(d *internal.Option) {
		d.AutoClean = true
	}
}

// Debug specify to enable debug mode of DB.
func Debug() internal.DBOption {
	return func(d *internal.Option) {
		d.Debug = true
	}
}

// KeyPathSep specify separater of key path for DB, default is ".".
func KeyPathSep(s string) internal.DBOption {
	return func(d *internal.Option) {
		d.KeyPathSep = s
	}
}

// DefaultLimit specify default limit of children pagination for DB.
func DefaultLimit(l int) internal.DBOption {
	return func(d *internal.Option) {
		d.DefaultLimit = l
	}
}
