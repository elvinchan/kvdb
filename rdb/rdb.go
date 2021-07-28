package rdb

import (
	"errors"
	"log"
	"os"
	"time"

	"github.com/elvinchan/kvdb"
	"github.com/elvinchan/kvdb/internal"
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
	db      *gorm.DB
	option  *kvdb.Option
	loadRec *internal.LoadRec
	close   chan struct{}
}

type rdbNode struct {
	Key       string    `gorm:"primaryKey"`
	ParentKey string    `gorm:"index"`
	Value     string    `gorm:"type:text"`
	ExpireAt  time.Time `gorm:"index"`
}

type DriverType int

const (
	DriverSqlite3 DriverType = iota
	DriverMySQL
	DriverPostgres
)

func NewDB(driver DriverType, dsn string, opts ...kvdb.DBOption) (kvdb.KVDB, error) {
	o := kvdb.InitOption()
	for _, opt := range opts {
		opt(o)
	}
	logLevel := logger.Silent
	if o.Debug {
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
	if err = db.AutoMigrate(&rdbNode{}); err != nil {
		return nil, err
	}
	v := rdb{
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
	return &v, nil
}

func (g *rdb) Get(key string, opts ...kvdb.GetOption) (*kvdb.Node, error) {
	var gt kvdb.Getter
	for _, opt := range opts {
		opt(&gt)
	}
	if gt.Children && gt.Limit == 0 {
		gt.Limit = g.option.DefaultLimit
	}
	now := time.Now()
	defer g.hookReq(time.Since(now))
	var row rdbNode
	err := g.db.Where("expire_at > ?", now).Where("key = ?", key).
		Take(&row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	node := kvdb.Node{
		Value: row.Value,
	}
	if gt.Children {
		var rows []rdbNode
		err = g.db.Where("parent_key = ?", key).
			Where("key > ?", g.option.FullKey(
				g.option.BareKey(gt.Start), row.Key),
			).
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

func (g *rdb) GetMulti(keys []string, opts ...kvdb.GetOption,
) (map[string]kvdb.Node, error) {
	var gt kvdb.Getter
	for _, opt := range opts {
		opt(&gt)
	}
	if gt.Children && gt.Limit == 0 {
		gt.Limit = g.option.DefaultLimit
	}
	if len(keys) == 0 {
		return nil, nil
	}
	now := time.Now()
	defer g.hookReq(time.Since(now))
	var rows []rdbNode
	err := g.db.Where("expire_at > ?", now).
		Where("key IN ?", keys).
		Find(&rows).Error
	if err != nil {
		return nil, err
	}

	isBareStartKey := g.option.IsBareKey(gt.Start)
	parentStartKey := g.option.ParentKey(gt.Start)
	v := make(map[string]kvdb.Node, len(rows))
	for _, row := range rows {
		node := kvdb.Node{
			Value: row.Value,
		}
		if gt.Children && (isBareStartKey || parentStartKey == row.Key) {
			var rows []rdbNode
			err = g.db.Where("parent_key = ?", row.Key).
				Where("key > ?", g.option.FullKey(
					g.option.BareKey(gt.Start), row.Key),
				).
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
		v[row.Key] = node
	}
	return v, nil
}

func (g *rdb) Set(key, value string, opts ...kvdb.SetOption) error {
	var st kvdb.Setter
	for _, opt := range opts {
		opt(&st)
	}
	if st.ExpireAt.IsZero() {
		st.ExpireAt = maxDatetime
	}
	now := time.Now()
	defer g.hookReq(time.Since(now))
	row := rdbNode{
		Key:       key,
		ParentKey: g.option.ParentKey(key),
		Value:     value,
		ExpireAt:  st.ExpireAt,
	}
	return g.db.Clauses(clause.OnConflict{
		UpdateAll: true,
	}).Create(&row).Error
}

func (g *rdb) SetMulti(kvPairs []string, opts ...kvdb.SetOption) error {
	var st kvdb.Setter
	for _, opt := range opts {
		opt(&st)
	}
	if st.ExpireAt.IsZero() {
		st.ExpireAt = maxDatetime
	}
	if len(kvPairs)%2 != 0 {
		return kvdb.ErrorKeyValuePairs
	}
	now := time.Now()
	defer g.hookReq(time.Since(now))
	var rows []rdbNode
	for i := 0; i < len(kvPairs)/2; i++ {
		rows = append(rows, rdbNode{
			Key:       kvPairs[i*2],
			ParentKey: g.option.ParentKey(kvPairs[i*2]),
			Value:     kvPairs[i*2+1],
			ExpireAt:  st.ExpireAt,
		})
	}
	return g.db.Clauses(clause.OnConflict{
		UpdateAll: true,
	}).Create(&rows).Error
}

func (g *rdb) Delete(key string, opts ...kvdb.DeleteOption) error {
	var dt kvdb.Deleter
	for _, opt := range opts {
		opt(&dt)
	}
	now := time.Now()
	defer g.hookReq(time.Since(now))
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
	if len(keys) == 0 {
		return nil
	}
	now := time.Now()
	defer g.hookReq(time.Since(now))
	query := g.db.Where("key IN ?", keys)
	if dt.Children {
		query.Or("parent_key IN ?", keys)
	}
	return query.Delete(&rdbNode{}).Error
}

func (g *rdb) Exist(key string) (bool, error) {
	now := time.Now()
	defer g.hookReq(time.Since(now))
	var cnt int64
	err := g.db.Model(&rdbNode{}).Where("key = ?", key).Count(&cnt).Error
	return cnt > 0, err
}

func (g *rdb) Cleanup() error {
	return g.db.Where("expire_at <= ?", time.Now()).Delete(&rdbNode{}).Error
}

func (g *rdb) Close() error {
	close(g.close)
	return nil
}

func (g *rdb) hookReq(score time.Duration) {
	if g.option.AutoClean {
		g.loadRec.HookReq(int64(score))
	}
}
