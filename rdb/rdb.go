package rdb

import (
	"errors"
	"log"
	"os"
	"time"

	"github.com/elvinchan/kvdb"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

var (
	maxDatetime, _  = time.Parse("2006-01-02 15:04:05", "9999-12-31 23:59:59")
	UnsupportDriver = errors.New("unsupport driver")
)

type rdb struct {
	db        *gorm.DB
	autoClean bool
}

type rdbNode struct {
	Key       string    `gorm:"primaryKey"`
	ParentKey string    `gorm:"index"`
	Value     string    `gorm:"type:text"`
	ExpireAt  time.Time `gorm:"index"`
}

const (
	DriverSqlite3 = iota
	DriverMySQL
	DriverPostgres
)

func NewDB(driver int, dsn string, opts ...kvdb.DBOption) (kvdb.KVDB, error) {
	var d kvdb.DB
	for _, opt := range opts {
		opt(&d)
	}
	logLevel := logger.Silent
	if d.Debug {
		logLevel = logger.Info
	}
	gormLogger := logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags), // io writer
		logger.Config{
			SlowThreshold:             time.Second, // Slow SQL threshold
			LogLevel:                  logLevel,    // Log level
			IgnoreRecordNotFoundError: true,        // Ignore ErrRecordNotFound error for logger
		},
	)
	var gormDialer gorm.Dialector
	switch driver {
	case DriverSqlite3:
		gormDialer = sqlite.Open(dsn)
	case DriverMySQL:
		gormDialer = mysql.Open(dsn)
	case DriverPostgres:
		gormDialer = postgres.Open(dsn)
	default:
		return nil, UnsupportDriver
	}
	db, err := gorm.Open(gormDialer, &gorm.Config{
		Logger: gormLogger,
	})
	if err != nil {
		return nil, err
	}
	err = db.AutoMigrate(&rdbNode{})
	return &rdb{
		db:        db,
		autoClean: d.AutoClean,
	}, err
}

func (g *rdb) Get(key string, opts ...kvdb.GetOption) (*kvdb.Node, error) {
	var gt kvdb.Getter
	for _, opt := range opts {
		opt(&gt)
	}
	now := time.Now()
	row := rdbNode{Key: key}
	err := g.db.Where("expire_at > ?", now).Take(&row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	node := kvdb.Node{
		Value: row.Value,
	}
	if gt.Limit > 0 {
		var rows []rdbNode
		err = g.db.Where("parent_key = ?", key).
			Where("key > ?", gt.Start).
			Where("expire_at > ?", now).
			Limit(gt.Limit).
			Find(&rows).Error
		if err != nil {
			return nil, err
		}
		node.Children = make(map[string]string, len(rows))
		for i := range rows {
			node.Children[rows[i].Key] = rows[i].Value
		}
	}
	return &node, nil
}

func (g *rdb) GetMulti(keys []string, opts ...kvdb.GetOption) ([]kvdb.Node, error) {
	var gt kvdb.Getter
	for _, opt := range opts {
		opt(&gt)
	}
	now := time.Now()
	var rows []rdbNode
	err := g.db.Where("expire_at > ?", now).
		Where("key IN ?", keys).
		Find(&rows).Error
	if err != nil {
		return nil, err
	}

	bareStartKey := kvdb.IsBareKey(gt.Start)
	parentStartKey := kvdb.ParseParentKey(gt.Start)
	nodes := make([]kvdb.Node, len(rows))
	for _, row := range rows {
		node := kvdb.Node{
			Value: row.Value,
		}
		if gt.Limit > 0 &&
			(bareStartKey || parentStartKey == row.Key) {
			var rows []rdbNode
			err = g.db.Where("parent_key = ?", row.Key).
				Where("key > ?", kvdb.FullKey(gt.Start, row.Key)).
				Where("expire_at > ?", now).
				Limit(gt.Limit).
				Find(&rows).Error
			if err != nil {
				return nil, err
			}
			node.Children = make(map[string]string, len(rows))
			for i := range rows {
				node.Children[rows[i].Key] = rows[i].Value
			}
		}
		nodes = append(nodes, node)
	}
	return nodes, nil
}

func (g *rdb) Set(key, value string, opts ...kvdb.SetOption) error {
	var st kvdb.Setter
	st.ExpireAt = maxDatetime
	for _, opt := range opts {
		opt(&st)
	}
	row := rdbNode{
		Key:       key,
		ParentKey: kvdb.ParseParentKey(key),
		Value:     value,
		ExpireAt:  st.ExpireAt,
	}
	return g.db.Clauses(clause.OnConflict{
		UpdateAll: true,
	}).Create(&row).Error
}

func (g *rdb) SetMulti(kvPairs []string, opts ...kvdb.SetOption) error {
	var st kvdb.Setter
	st.ExpireAt = maxDatetime
	for _, opt := range opts {
		opt(&st)
	}
	if len(kvPairs)%2 != 0 {
		return errors.New("invalid key value pairs count")
	}
	var rows []rdbNode
	for i := 0; i < len(kvPairs)/2; i++ {
		rows = append(rows, rdbNode{
			Key:       kvPairs[i*2],
			ParentKey: kvdb.ParseParentKey(kvPairs[i*2]),
			Value:     kvPairs[i*2+1],
			ExpireAt:  st.ExpireAt,
		})
	}
	return g.db.Clauses(clause.OnConflict{
		UpdateAll: true,
	}).Create(&rows).Error
}

// DEL node:key SREM children:key
func (g *rdb) Delete(key string, opts ...kvdb.DeleteOption) error {
	var dt kvdb.Deleter
	for _, opt := range opts {
		opt(&dt)
	}
	query := g.db.Where("key = ?", key)
	if dt.Children {
		query.Or("parent_key = ?", key)
	}
	return query.Delete(&rdbNode{}).Error
}

func (g *rdb) DeleteMulti(keys []string, opts ...kvdb.DeleteOption) error {
	var dt kvdb.Deleter
	for _, opt := range opts {
		opt(&dt)
	}
	query := g.db.Where("key IN ?", keys)
	if dt.Children {
		query.Or("parent_key IN ?", keys)
	}
	return query.Delete(&rdbNode{}).Error
}

func (g *rdb) Exist(key string) (bool, error) {
	var cnt int64
	err := g.db.Model(&rdbNode{}).Where("key = ?", key).Count(&cnt).Error
	return cnt > 0, err
}

func (g *rdb) Close() error {
	return nil
}

// Cleanup removes expired keys
func (g *rdb) Cleanup() error {
	return g.db.Where("expire_at <= ?", time.Now()).Error
}
