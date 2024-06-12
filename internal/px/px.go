package px

import (
	"bytes"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/gernest/frieren/internal/blob"
)

type Ctx struct {
	o   roaring64.Bitmap
	tr  blob.Func
	buf bytes.Buffer
}

func (x *Ctx) Tr(k []byte) uint64 {
	return x.tr(k)
}

func (x *Ctx) Or(o *Ctx) {
	x.o.Or(&o.o)
}

func (x *Ctx) ToArray() []uint64 {
	return x.o.ToArray()
}

func New(tr blob.Func) *Ctx {
	return &Ctx{}
}

func (x *Ctx) Reset() {
	x.o.Clear()
	x.buf.Reset()
}

func (x *Ctx) Set(key, value string) {
	x.buf.Reset()
	x.buf.WriteString(key)
	x.buf.WriteByte('=')
	x.buf.WriteString(value)
	x.o.Add(x.tr(x.buf.Bytes()))
}
