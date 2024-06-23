package keys

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"sync"

	"github.com/gernest/frieren/internal/constants"
)

const (
	seq uint64 = iota
	blobSeq
	blobID
	blobHash
	fst
	metadata
	fieldView
)

func Seq(field constants.ID, view string) []byte {
	a := get()
	defer a.Release()
	b := a[:]
	copy(b, view)
	b = append(b[:len(view)], ':', '0', ':')
	b = strconv.AppendUint(b, uint64(field), 10)
	return bytes.Clone(b)
}

const bufferSize = 16 + //for view
	2 + // prefix
	14 + // blob id
	2 // field

type buffer [bufferSize]byte

var bufferPool = &sync.Pool{New: func() any {
	var b buffer
	return &b
}}

func get() *buffer {
	return bufferPool.Get().(*buffer)
}

func (b *buffer) Release() {
	clear(b[:])
	bufferPool.Put(b)
}

func BlobID(field constants.ID, id uint64, view string) []byte {
	a := get()
	defer a.Release()

	b := a[:]
	copy(b, view)
	b = append(b[:len(view)], ':', '2', ':')
	b = strconv.AppendUint(b, uint64(field), 10)
	b = append(b, ':')
	b = strconv.AppendUint(b, id, 10)
	return bytes.Clone(b)
}

func BlobHash(field constants.ID, hash uint64, view string) []byte {
	a := get()
	b := a[:]
	copy(b, view)
	b = append(b[:len(view)], ':', '3', ':')
	b = strconv.AppendUint(b, uint64(field), 10)
	b = append(b, ':')
	b = binary.AppendUvarint(b, hash)
	return bytes.Clone(b)
}

func FST(field constants.ID, shard uint64, view string) []byte {
	a := get()
	defer a.Release()
	b := a[:]
	copy(b, view)
	b = append(b[:len(view)], ':', '4', ':')
	b = strconv.AppendUint(b, uint64(field), 10)
	b = append(b, ':')
	b = strconv.AppendUint(b, shard, 10)
	return b
}

func Metadata(b *bytes.Buffer, name string) []byte {
	b.Reset()
	fmt.Fprintf(b, "%d:%s", metadata, name)
	return b.Bytes()
}

func FieldView(b *bytes.Buffer, resource constants.Resource, view string) []byte {
	b.Reset()
	fmt.Fprintf(b, "%s:%d:%d", view, resource, fieldView)
	return b.Bytes()
}
