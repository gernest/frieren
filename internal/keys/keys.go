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
	fstBitmap
	fst
	metadata
	fieldView
)

func Seq(b *bytes.Buffer, field constants.ID) []byte {
	b.Reset()
	fmt.Fprintf(b, "%d:%d:", blobID, field)
	return b.Bytes()
}

func BlobID(b *bytes.Buffer, field constants.ID, id uint64) []byte {
	b.Reset()
	fmt.Fprintf(b, "%d:%d:%d", blobID, field, id)
	return b.Bytes()
}

func BlobHash(b *bytes.Buffer, field constants.ID, hash uint64) []byte {
	b.Reset()
	fmt.Fprintf(b, "%d:%d:%d", blobHash, field, hash)
	return b.Bytes()
}

func FSTBitmap(b *bytes.Buffer, field constants.ID, shard uint64, view string) []byte {
	b.Reset()
	fmt.Fprintf(b, "%d:%d:%d:%s", fstBitmap, field, shard, view)
	return b.Bytes()
}

func FST(b *bytes.Buffer, field constants.ID, shard uint64, view string) []byte {
	b.Reset()
	fmt.Fprintf(b, "%d:%d:%d:%s", fst, field, shard, view)
	return b.Bytes()
}

func Metadata(b *bytes.Buffer, name string) []byte {
	b.Reset()
	fmt.Fprintf(b, "%d:%s", metadata, name)
	return b.Bytes()
}

func FieldView(b *bytes.Buffer, view string) []byte {
	b.Reset()
	fmt.Fprintf(b, "%d:%s", fieldView, view)
	return b.Bytes()
}
