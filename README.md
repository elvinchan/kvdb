# KVDB
A tree like structure key value database powered by SQLite, LevelDB, MongoDB etc

[![Ci](https://github.com/elvinchan/kvdb/actions/workflows/ci.yml/badge.svg)](https://github.com/elvinchan/kvdb/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/elvinchan/kvdb/branch/master/graph/badge.svg)](https://codecov.io/gh/elvinchan/kvdb)
[![Go Report Card](https://goreportcard.com/badge/github.com/elvinchan/kvdb)](https://goreportcard.com/report/github.com/elvinchan/kvdb)
[![Go Reference](https://pkg.go.dev/badge/github.com/elvinchan/kvdb.svg)](https://pkg.go.dev/github.com/elvinchan/kvdb)
[![MPLv2 License](https://img.shields.io/badge/license-MPLv2-blue.svg)](https://www.mozilla.org/MPL/2.0/)

## Getting Started

### Simple local usage
```go
// Create A KVDB instance using RDB or LevelDB backend
db, err := rdb.NewDB(rdb.DriverSqlite3, "sqlite.db")
if err != nil {
    panic(err)
}
// Use this instance for `Get/Set/Delete/...` operations
err = db.Set("k", "v")
if err != nil {
    panic(err)
}
rst, err := db.Get("k")
if err != nil {
    panic(err)
}
fmt.Println("value is:", rst.Value) // should be v
```

### Service usage
Some DB like SQLite and LevelDB does not provide a server for remote connect, which means unavailable for a common data source for distributed services. KVDB provide a service layer so you can easily use it in other process or a remote program.

- Server side
```go
db, err := rdb.NewDB(rdb.DriverSqlite3, "sqlite.db")
if err != nil {
    panic(err)
}
// this line will block your goroutine and start serving KVDB by provided
// network and address
err = server.StartServer(db, "tcp", ":9090")
if err != nil {
    panic(err)
}
```

- Client side
```go
db, err := service.DialKVDBService("tcp", ":9090")
if err != nil {
    panic(err)
}
// just use db like local KVDB instance, for example
err = db.Set("k", "v")
if err != nil {
    panic(err)
}
rst, err := db.Get("k")
if err != nil {
    panic(err)
}
fmt.Println("value is:", rst.Value) // should be v
```

### Tree like structure
KVDB treat key also as path of key tree, for example, for a key tree like this:  
```
        a  
        |  
     /     \  
   b1       b2  
   |        |  
 /   \    /   \  
c1   c2  c3   c4  
```

Should use follow keys:  
```
a  
a.b1  
a.b2  
a.b1.c1  
a.b1.c2  
a.b2.c3  
a.b2.c4  
```

Then when using `Get/GetMulti`, you can also retrieve children (not grand children) key-values  
```go
rst, err := db.Get("a", kvdb.GetChildren("", 2))
if err != nil {
    panic(err)
}
fmt.Println("length of children of result:", len(rst.Children)) // should be 2
for k, v := range rst.Children {
    fmt.Printf("Child key: %s, value: %s", k, v)
    // should ouput a.b1 and a.b2 with it's value
}
```

### TTL
KVDB support time to live for key, you can set expire time when using `Set/SetMulti`
```go
err = db.Set("k", "v",  kvdb.SetExpire(time.Now()))
if err != nil {
    panic(err)
}
time.Sleep(time.Second)
rst, err := db.Get("k")
if err != nil {
    panic(err)
}
fmt.Println("result is:", rst) // should be nil
```

KVDB would not delete expired key-value data until you called `Cleanup`. Or you can enable auto cleanup, which will cleanup periodically.
```go
rdb.NewDB(rdb.DriverSqlite3, "sqlite.db", kvdb.AutoClean())
```

## License

[MIT](https://github.com/elvinchan/kvdb/blob/master/LICENSE)
