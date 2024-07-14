package store

import (
	"hash"
	"hash/crc32"
	"hash/maphash"
	"sync"
)

var pool32 = &sync.Pool{New: func() any {
	return &hs{Hash32: crc32.NewIEEE()}
}}

type hs struct {
	hash.Hash32
}

var pool64 = &sync.Pool{New: func() any {
	return new(maphash.Hash)
}}

func Hash32(f func(h hash.Hash32)) (o uint32) {
	h := pool32.Get().(*hs)
	f(h)
	o = h.Sum32()
	h.Reset()
	pool32.Put(h)
	return
}

func Hash64(data []byte) (o uint64) {
	h := pool64.Get().(*maphash.Hash)
	h.Write(data)
	o = h.Sum64()
	h.Reset()
	pool64.Put(h)
	return
}
