package storage

import (
	"sync"
	"time"
)

var (
	entryPool = sync.Pool{
		New: func() interface{} {
			return &Entry{}
		},
	}

	entrySlicePool = sync.Pool{
		New: func() interface{} {
			return make([]Entry, 0, 64)
		},
	}

	resultMapPool = sync.Pool{
		New: func() interface{} {
			return make(map[string]Entry)
		},
	}

	bytesBufferPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 0, 4096)
		},
	}
)

func GetEntry() *Entry {
	return entryPool.Get().(*Entry)
}

func PutEntry(e *Entry) {
	if e == nil {
		return
	}
	e.Key = ""
	e.Value = nil
	e.Version = 0
	e.TimeStamp = time.Time{}
	e.Tombstone = false
	e.VectorClock = nil
	entryPool.Put(e)
}

func GetEntrySlice() []Entry {
	return entrySlicePool.Get().([]Entry)
}

func PutEntrySlice(s []Entry) {
	if s == nil {
		return
	}
	for i := range s {
		s[i] = Entry{}
	}
	entrySlicePool.Put(s[:0])
}

func GetResultMap() map[string]Entry {
	return resultMapPool.Get().(map[string]Entry)
}

func PutResultMap(m map[string]Entry) {
	if m == nil {
		return
	}
	for k := range m {
		delete(m, k)
	}
	resultMapPool.Put(m)
}

func GetBytesBuffer() []byte {
	return bytesBufferPool.Get().([]byte)
}

func PutBytesBuffer(b []byte) {
	if b == nil {
		return
	}
	bytesBufferPool.Put(b[:0])
}
