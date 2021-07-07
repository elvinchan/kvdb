package internal

import (
	"sync"
	"testing"
	"time"
)

func TestHookReq(t *testing.T) {
	expect := 100
	var r LoadRec
	var wg sync.WaitGroup
	for i := 0; i < expect; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.HookReq(1)
		}()
	}
	wg.Wait()

	if r.records[0] != int64(expect) {
		t.Errorf("records not right, expect %d, got %d", expect, r.records[0])
		t.Fail()
	}
}

func TestTick(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		expect := 100
		var r LoadRec
		for i := 0; i < expect; i++ {
			go func() {
				if !r.Tick() {
					t.Errorf("Tick() result not right, expect %v, got %v", true, false)
					t.Fail()
				}
			}()
		}
	})

	t.Run("Simulate", func(t *testing.T) {
		var r LoadRec
		for i := 0; i < 3; i++ {
			r.records[r.cur] = 100
			if !r.Tick() {
				t.Errorf("Tick() result not right, expect %v, got %v", true, false)
				t.Fail()
			}
		}
		r.records[r.cur] = 1000
		if r.Tick() {
			t.Errorf("Tick() result not right, expect %v, got %v", false, true)
			t.Fail()
		}
		r.records[r.cur] = 50
		if !r.Tick() {
			t.Errorf("Tick() result not right, expect %v, got %v", true, false)
			t.Fail()
		}
		for i := 0; i < 60; i++ {
			r.records[r.cur] = 100
			if !r.Tick() {
				t.Errorf("Tick() result not right, expect %v, got %v", true, false)
				t.Fail()
			}
		}
		r.records[r.cur] = 1000
		if r.Tick() {
			t.Errorf("Tick() result not right, expect %v, got %v", false, true)
			t.Fail()
		}
		r.records[r.cur] = 50
		if !r.Tick() {
			t.Errorf("Tick() result not right, expect %v, got %v", true, false)
			t.Fail()
		}
	})
}

func TestStartClean(t *testing.T) {
	var (
		r = LoadRec{
			recordCycle: time.Millisecond,
			cleanPeriod: 5,
		}
		cleanCnt int
		close    = make(chan struct{})
	)
	go r.StartClean(func() {
		cleanCnt++
	}, close)

	time.Sleep(r.recordCycle * time.Duration(r.cleanPeriod*2))
	close <- struct{}{}
	if cleanCnt == 0 {
		t.Error("clean count not right, expect > 0, got 0")
		t.Fail()
	}

	before := cleanCnt
	time.Sleep(r.recordCycle * time.Duration(r.cleanPeriod*2))
	after := cleanCnt
	if before != after {
		t.Errorf("clean count not right, expect %v, got %v", before, after)
		t.Fail()
	}
}
