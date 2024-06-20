package keys

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"

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

func Seq(b *bytes.Buffer, field constants.ID, view string) []byte {
	b.Reset()
	fmt.Fprintf(b, "%s:%d:%d:", view, seq, field)
	return b.Bytes()
}

const BufferSize = 16 + //for view
	2 + // prefix
	14 + // blob id
	2 // field

type Buffer [BufferSize]byte

func NewBuffer() *Buffer {
	var b Buffer
	return &b
}

func BlobID(a *Buffer, field constants.ID, id uint64, view string) []byte {
	b := a[:]
	copy(b, view)
	b = append(b[:len(view)], ':', '2', ':')
	b = strconv.AppendUint(b, uint64(field), 10)
	b = append(b, ':')
	return strconv.AppendUint(b, id, 10)
}

func BlobHash(a *Buffer, field constants.ID, hash uint64, view string) []byte {
	b := a[:]
	copy(b, view)
	b = append(b[:len(view)], ':', '3', ':')
	b = strconv.AppendUint(b, uint64(field), 10)
	b = append(b, ':')
	return binary.AppendUvarint(b, hash)
}

func FST(b *bytes.Buffer, field constants.ID, shard uint64, view string) []byte {
	b.Reset()
	fmt.Fprintf(b, "%s:%d:%d:%d", view, fst, field, shard)
	return b.Bytes()
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
