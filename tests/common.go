package tests

import (
	"strconv"
	"testing"

	"github.com/elvinchan/kvdb"
)

var DefaultLimit = 2

func TestGetSet(t *testing.T, newDB func() (kvdb.KVDB, error)) {
	kvs := []string{
		"group.g", "1",
		"group.g.child1", "2",
		"group.g.child2", "3",
		"group.g.child3", "4",
		"group.g.child4", "5",
		"group.g.child3.grandchild1", "6",
		"group.g.child3.grandchild2", "7",
		"group.g.child3.grandchild3", "8",
	}
	t.Run("Simple", func(t *testing.T) {
		db, err := newDB()
		if err != nil {
			panic(err)
		}
		defer func() {
			err := db.Close()
			if err != nil {
				panic(err)
			}
		}()
		rst, err := db.Get(kvs[0])
		if err != nil {
			t.Error(err)
			t.Fail()
		}
		if rst != nil {
			t.Errorf("result not right, expect nil")
			t.Fail()
		}

		rsts, err := db.GetMulti(kvs)
		if err != nil {
			t.Error(err)
			t.Fail()
		}
		if len(rsts) != 0 {
			t.Errorf("length of results not right, expect 0")
			t.Fail()
		}

		value := "z"
		err = db.Set(kvs[0], "z")
		if err != nil {
			t.Error(err)
			t.Fail()
		}

		rst, err = db.Get(kvs[0])
		if err != nil {
			t.Error(err)
			t.Fail()
		}
		if rst == nil {
			t.Errorf("result not right, expect not nil")
			t.Fail()
		} else if rst.Value != value {
			t.Errorf("value not right, expect %s, got %s", value, rst.Value)
			t.Fail()
		} else if rst.Children != nil {
			t.Errorf("children not right, expect nil")
			t.Fail()
		}

		err = db.SetMulti(kvs)
		if err != nil {
			t.Error(err)
			t.Fail()
		}

		for i := 0; i < len(kvs); i += 2 {
			rst, err := db.Get(kvs[i])
			if err != nil {
				t.Error(err)
				t.Fail()
			}
			if rst == nil {
				t.Errorf("result not right, expect not nil")
				t.Fail()
			} else if rst.Value != kvs[i+1] {
				t.Errorf("value not right, expect %s, got %s",
					kvs[i+1], rst.Value)
				t.Fail()
			}
		}

		rsts, err = db.GetMulti(kvs)
		if err != nil {
			t.Error(err)
			t.Fail()
		}
		if rsts == nil {
			t.Errorf("result not right, expect not nil")
			t.FailNow()
		}
		for i := 0; i < len(kvs); i += 2 {
			if rsts[kvs[i]].Value != kvs[i+1] {
				t.Errorf("value not right, expect %s, got %s",
					kvs[i+1], rsts[kvs[i]].Value)
				t.Fail()
			}
		}
	})

	t.Run("WithChildren", func(t *testing.T) {
		db, err := newDB()
		if err != nil {
			panic(err)
		}
		defer func() {
			err := db.Close()
			if err != nil {
				panic(err)
			}
		}()
		err = db.SetMulti(kvs)
		if err != nil {
			t.Error(err)
			t.Fail()
		}

		cases := []struct {
			KeyGet                    int
			Start                     string
			Limit                     int
			KeyRstIndex, KeyRstLength int
		}{
			{0, "", -1, 1, 4},
			{0, "", 1, 1, 1},
			{0, "", 0, 1, 2},
			{0, "group.g.child1", -1, 2, 3},
			{0, "group.g.child1", 0, 2, 2},
			{0, "group.g.child1", 1, 2, 1},
			{2, "", -1, 0, 0},
			{3, "", 2, 5, 2},
			{3, "group.g.child3.grandchild2", 2, 7, 1},
			{3, "group.g.child3.grandchild3", -1, 0, 0},
		}

		for i, c := range cases {
			t.Run(strconv.Itoa(i), func(t *testing.T) {
				rst, err := db.Get(kvs[c.KeyGet*2], kvdb.GetChildren(c.Start, c.Limit))
				if err != nil {
					t.Error(err)
					t.Fail()
				}
				if rst == nil {
					t.Errorf("result not right, expect not nil")
					t.Fail()
				} else if len(rst.Children) != c.KeyRstLength {
					t.Errorf("length of children not right, expect %v, got %v",
						c.KeyRstLength, len(rst.Children))
					t.Fail()
				} else if len(rst.Children) > 0 {
					for j := c.KeyRstIndex; j < c.KeyRstIndex+c.KeyRstLength; j++ {
						if rst.Children[kvs[j*2]] != kvs[j*2+1] {
							t.Errorf("value not right, expect %s, got %s",
								kvs[i*2+1], rst.Children[kvs[j*2]])
							t.Fail()
						}
					}
				}
			})
		}
	})

	t.Run("MultiWithChildren", func(t *testing.T) {
		db, err := newDB()
		if err != nil {
			panic(err)
		}
		defer func() {
			err := db.Close()
			if err != nil {
				panic(err)
			}
		}()
		err = db.SetMulti(kvs)
		if err != nil {
			t.Error(err)
			t.Fail()
		}

		cases := []struct {
			Mapping map[int][]int
			Start   string
			Limit   int
		}{
			{map[int][]int{
				0: {1, 2, 3, 4},
				1: {},
				3: {5, 6, 7},
				5: {},
			}, "", -1},
			{map[int][]int{
				0: {4},
				1: {},
				3: {},
				5: {},
			}, "group.g.child3", -1},
			{map[int][]int{
				0: {},
				1: {},
				3: {6, 7},
				5: {},
			}, "group.g.child3.grandchild1", -1},
			{map[int][]int{
				0: {},
				1: {},
				3: {6},
				5: {},
			}, "group.g.child3.grandchild1", 1},
		}

		for i, c := range cases {
			t.Run(strconv.Itoa(i), func(t *testing.T) {
				var keys []string
				for mk := range c.Mapping {
					keys = append(keys, kvs[mk*2])
				}
				rsts, err := db.GetMulti(
					keys,
					kvdb.GetChildren(c.Start, c.Limit),
				)
				if err != nil {
					t.Error(err)
					t.Fail()
				}
				if len(rsts) != len(c.Mapping) {
					t.Errorf("length of results not right, expect %v, got %v",
						len(c.Mapping), len(rsts))
					t.Fail()
				} else {
					for mk, mv := range c.Mapping {
						rst := rsts[kvs[mk*2]]
						if rst.Value != kvs[mk*2+1] {
							t.Errorf("value not right, expect %s, got %s",
								kvs[mk*2+1], rst.Value)
							t.Fail()
						} else if len(rst.Children) != len(mv) {
							t.Errorf("length of children not right, expect %v, got %v",
								len(mv), len(rst.Children))
							t.Fail()
						} else if len(rst.Children) > 0 {
							for _, mvt := range mv {
								if rst.Children[kvs[mvt*2]] != kvs[mvt*2+1] {
									t.Errorf("value not right, expect %s, got %s",
										kvs[mvt*2+1], rst.Children[kvs[mvt*2]])
									t.Fail()
								}
							}
						}
					}
				}
			})
		}
	})
}

func TestDelete(t *testing.T, newDB func() (kvdb.KVDB, error)) {
	kvs := []string{
		"group.d", "1",
		"group.d.child1", "2",
		"group.d.child2", "3",
		"group.d.child3", "4",
		"group.d.child3.grandchild1", "5",
		"group.d.child3.grandchild2", "6",
	}
	var keys []string
	for i := 0; i < len(kvs); i += 2 {
		keys = append(keys, kvs[i])
	}

	setData := func() kvdb.KVDB {
		db, err := newDB()
		if err != nil {
			panic(err)
		}
		err = db.SetMulti(kvs)
		if err != nil {
			t.Error(err)
			t.Fail()
		}
		rsts, err := db.GetMulti(keys)
		if err != nil {
			t.Error(err)
			t.Fail()
		}
		if rsts == nil {
			t.Errorf("value not right, expect not nil")
			t.Fail()
		}
		for i := 0; i < len(kvs); i += 2 {
			if rsts[kvs[i]].Value != kvs[i+1] {
				t.Errorf("value not right, expect %s, got %s",
					kvs[i+1], rsts[kvs[i]].Value)
				t.Fail()
			}
		}
		return db
	}

	t.Run("Delete", func(t *testing.T) {
		db := setData()
		defer func() {
			err := db.Close()
			if err != nil {
				panic(err)
			}
		}()

		err := db.Delete(kvs[0])
		if err != nil {
			t.Error(err)
			t.Fail()
		}
		for i := 0; i < len(kvs); i += 2 {
			rst, err := db.Get(kvs[i])
			if err != nil {
				t.Error(err)
				t.Fail()
			}
			if i == 0 {
				if rst != nil {
					t.Errorf("result not right, expect nil")
					t.Fail()
				}
			} else {
				if rst == nil {
					t.Errorf("result not right, expect not nil")
					t.Fail()
				} else if rst.Value != kvs[i+1] {
					t.Errorf("value not right, expect %v, got %v", kvs[i+1], rst.Value)
					t.Fail()
				}
			}
		}

		err = db.Delete(kvs[8])
		if err != nil {
			t.Error(err)
			t.Fail()
		}
		for i := 0; i < len(kvs); i = i + 2 {
			rst, err := db.Get(kvs[i])
			if err != nil {
				t.Error(err)
				t.Fail()
			}
			if i == 0 || i == 8 {
				if rst != nil {
					t.Errorf("result not right, expect nil")
					t.Fail()
				}
			} else {
				if rst == nil {
					t.Errorf("result not right, expect not nil")
					t.Fail()
				} else if rst.Value != kvs[i+1] {
					t.Errorf("value not right, expect %v, got %v", kvs[i+1], rst.Value)
					t.Fail()
				}
			}
		}
	})

	t.Run("DeleteWithChildren", func(t *testing.T) {
		db := setData()
		defer func() {
			err := db.Close()
			if err != nil {
				panic(err)
			}
		}()

		err := db.Delete(kvs[0], kvdb.DeleteChildren())
		if err != nil {
			t.Error(err)
			t.Fail()
		}
		for i := 0; i < len(kvs); i += 2 {
			rst, err := db.Get(kvs[i])
			if err != nil {
				t.Error(err)
				t.Fail()
			}
			if i/2 < 4 {
				// self and children
				if rst != nil {
					t.Errorf("result not right, expect nil")
					t.Fail()
				}
			} else {
				// grandchildren
				if rst == nil {
					t.Errorf("result not right, expect not nil")
					t.Fail()
				} else if rst.Value != kvs[i+1] {
					t.Errorf("value not right, expect %v, got %v", kvs[i+1], rst.Value)
					t.Fail()
				}
			}
		}
	})

	t.Run("DeleteMulti", func(t *testing.T) {
		db := setData()
		defer func() {
			err := db.Close()
			if err != nil {
				panic(err)
			}
		}()

		err := db.DeleteMulti([]string{kvs[0], kvs[8]})
		if err != nil {
			t.Error(err)
			t.Fail()
		}
		for i := 0; i < len(kvs); i = i + 2 {
			rst, err := db.Get(kvs[i])
			if err != nil {
				t.Error(err)
				t.Fail()
			}
			if i == 0 || i == 8 {
				if rst != nil {
					t.Errorf("result not right, expect nil")
					t.Fail()
				}
			} else {
				if rst == nil {
					t.Errorf("result not right, expect not nil")
					t.Fail()
				} else if rst.Value != kvs[i+1] {
					t.Errorf("value not right, expect %v, got %v", kvs[i+1], rst.Value)
					t.Fail()
				}
			}
		}
	})

	t.Run("DeleteMultiWithChildren", func(t *testing.T) {
		db := setData()
		defer func() {
			err := db.Close()
			if err != nil {
				panic(err)
			}
		}()

		err := db.DeleteMulti([]string{kvs[0], kvs[8]}, kvdb.DeleteChildren())
		if err != nil {
			t.Error(err)
			t.Fail()
		}
		for i := 0; i < len(kvs); i = i + 2 {
			rst, err := db.Get(kvs[i])
			if err != nil {
				t.Error(err)
				t.Fail()
			}
			if i != 10 {
				if rst != nil {
					t.Errorf("result not right, expect nil")
					t.Fail()
				}
			} else {
				if rst == nil {
					t.Errorf("result not right, expect not nil")
					t.Fail()
				} else if rst.Value != kvs[i+1] {
					t.Errorf("value not right, expect %v, got %v", kvs[i+1], rst.Value)
					t.Fail()
				}
			}
		}
	})
}

func TestExist(t *testing.T, newDB func() (kvdb.KVDB, error)) {
	db, err := newDB()
	if err != nil {
		panic(err)
	}
	defer func() {
		err := db.Close()
		if err != nil {
			panic(err)
		}
	}()
	key := "group.e"
	has, err := db.Exist(key)
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	if has {
		t.Errorf("result of Exist() not right, expect %v, got %v", false, has)
		t.Fail()
	}

	err = db.Set(key, "1")
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	has, err = db.Exist(key)
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	if !has {
		t.Errorf("result of Exist() not right, expect %v, got %v", true, has)
		t.Fail()
	}
}
