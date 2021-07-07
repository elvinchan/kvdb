package internal

import (
	"sort"
	"sync"
	"time"
)

const (
	// record request numbers per record cycle, at least 2
	recordCount = 100
)

type LoadRec struct {
	records     [recordCount]int64
	recordCycle time.Duration
	cleanPeriod int
	cur         int
	repeat      bool
	mu          sync.Mutex
}

func DefaultLoadRec() *LoadRec {
	return &LoadRec{
		recordCycle: time.Minute,
		cleanPeriod: 10,
	}
}

func (r *LoadRec) HookReq(score int64) {
	r.mu.Lock()
	r.records[r.cur] += score
	r.mu.Unlock()
}

// Tick should be execute by cleanTickThreshold and returns if should do clean
func (r *LoadRec) Tick() bool {
	r.mu.Lock()
	shoudClean := !r.isHighLoad()
	r.cur++
	if r.cur == recordCount {
		r.cur = 0
		r.repeat = true
	}
	r.records[r.cur] = 0
	r.mu.Unlock()
	return shoudClean
}

func (r *LoadRec) StartClean(cleanup func(), close <-chan struct{}) {
	ticker := time.NewTicker(r.recordCycle)
	cleanTick := 0
	for {
		select {
		case <-ticker.C:
			cleanTick++
			if r.Tick() && cleanTick > r.cleanPeriod {
				cleanTick = 0
				cleanup()
			}
		case <-close:
			return
		}
	}
}

// high load == beyond 2 times median request count
func (r *LoadRec) isHighLoad() bool {
	if !r.repeat && r.cur < 1 {
		return false
	}
	var records []int64
	records = append(records, r.records[:r.cur]...)
	if r.repeat {
		records = append(records, r.records[r.cur+1:]...)
	}
	sortInt64s(records)
	median := records[len(records)/2]
	return r.records[r.cur] > median*2
}

func sortInt64s(a []int64) { sort.Sort(Int64Slice(a)) }

// Int64Slice attaches the methods of Interface to []int64, sorting in increasing order.
type Int64Slice []int64

func (p Int64Slice) Len() int           { return len(p) }
func (p Int64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p Int64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
