package px

import (
	"bytes"
	"crypto/sha512"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/gernest/frieren/internal/blob"
	"github.com/gernest/frieren/internal/constants"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

type Ctx struct {
	o   roaring64.Bitmap
	tr  blob.Func
	buf bytes.Buffer
	id  constants.ID
}

func (x *Ctx) ID(field constants.ID) uint64 {
	x.buf.Reset()
	x.o.RunOptimize()
	x.o.WriteTo(&x.buf)
	sum := sha512.Sum512(x.buf.Bytes())
	return x.tr(field, sum[:])
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
func (x *Ctx) Remove(values ...uint64) {
	for i := range values {
		x.o.Remove(values[i])
	}
}

func (x *Ctx) Bitmap() *roaring64.Bitmap {
	return &x.o
}

func New(id constants.ID, tr blob.Func) *Ctx {
	return &Ctx{id: id, tr: tr}
}

func (x *Ctx) Reset() {
	x.o.Clear()
	x.buf.Reset()
}

func (x *Ctx) Attr(prefix string) func(key string, value pcommon.Value) bool {
	return func(key string, value pcommon.Value) bool {
		return x.SetAttribute(prefix+key, value)
	}
}

func (x *Ctx) SetAttribute(key string, value pcommon.Value) bool {
	x.Set(key, value.AsString())
	return true
}

func (x *Ctx) Add(id uint64) {
	x.o.Add(id)
}

func (x *Ctx) Set(key, value string) uint64 {
	x.buf.Reset()
	x.buf.WriteString(key)
	x.buf.WriteByte('=')
	x.buf.WriteString(value)
	id := x.tr(x.id, bytes.Clone(x.buf.Bytes()))
	x.o.Add(id)
	return id
}
