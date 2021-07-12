package kvdb

import "testing"

func TestDBOptions(t *testing.T) {
	o := InitOption()

	parentBareKey := o.ParentBareKey("a.b.c")
	if parentBareKey != "b" {
		t.Errorf("parentBareKey not right, expect %s, got %s", "b", parentBareKey)
		t.Fail()
	}

	parentBareKey = o.ParentBareKey("a")
	if parentBareKey != "" {
		t.Errorf("parentBareKey not right, expect %s, got %s", "", parentBareKey)
		t.Fail()
	}

	bareKey := o.BareKey("a.b.c")
	if bareKey != "c" {
		t.Errorf("bareKey not right, expect %s, got %s", "c", bareKey)
		t.Fail()
	}

	bareKey = o.BareKey("a")
	if bareKey != "a" {
		t.Errorf("bareKey not right, expect %s, got %s", "a", bareKey)
		t.Fail()
	}

	isBareKey := o.IsBareKey("a.b.c")
	if isBareKey {
		t.Errorf("isBareKey not right, expect %v, got %v", false, isBareKey)
		t.Fail()
	}

	isBareKey = o.IsBareKey("c")
	if !isBareKey {
		t.Errorf("isBareKey not right, expect %v, got %v", true, isBareKey)
		t.Fail()
	}

	fullKey := o.FullKey("c", "a.b")
	if fullKey != "a.b.c" {
		t.Errorf("fullKey not right, expect %v, got %v", "a.b.c", fullKey)
		t.Fail()
	}

	fullKey = o.FullKey("c", "")
	if fullKey != "c" {
		t.Errorf("fullKey not right, expect %v, got %v", "c", fullKey)
		t.Fail()
	}
}
