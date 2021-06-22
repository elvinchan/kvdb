package rdb

import "testing"

func TestMaxDatetime(t *testing.T) {
	expect := "9999-12-31 23:59:59"
	str := maxDatetime.Format("2006-01-02 15:04:05")
	if str != expect {
		t.Errorf("max datetime not right, expect %s, got %s", expect, str)
		t.Fail()
	}
}
