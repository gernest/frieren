package keys

import (
	"bytes"
	"fmt"

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

func BlobID(b *bytes.Buffer, field constants.ID, id uint64, view string) []byte {
	b.Reset()
	fmt.Fprintf(b, "%s:%d:%d:%d", view, blobID, field, id)
	return b.Bytes()
}

func BlobHash(b *bytes.Buffer, field constants.ID, hash uint64, view string) []byte {
	b.Reset()
	fmt.Fprintf(b, "%s:%d:%d:%d", view, blobHash, field, hash)
	return b.Bytes()
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
