package px

import (
	"bytes"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/gernest/frieren/internal/blob"
	"github.com/gernest/frieren/internal/constants"
)

type Ctx struct {
	o   roaring64.Bitmap
	tr  blob.Func
	buf bytes.Buffer
	id  constants.ID
}

func (x *Ctx) Tr(k []byte) uint64 {
	return x.tr(x.id, k)
}

func (x *Ctx) Or(o *Ctx) {
	x.o.Or(&o.o)
}

func (x *Ctx) ToArray() []uint64 {
	return x.o.ToArray()
}
func (x *Ctx) Bitmap() *roaring64.Bitmap {
	return &x.o
}

func New(id constants.ID, tr blob.Func) *Ctx {
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
	x.o.Add(x.tr(x.id, x.buf.Bytes()))
}
