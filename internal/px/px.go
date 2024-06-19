package px

import (
	"bytes"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/gernest/frieren/internal/blob"
	"github.com/gernest/frieren/internal/constants"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

type Ctx struct {
	o    roaring64.Bitmap
	omit roaring64.Bitmap
	tr   blob.Func
	buf  bytes.Buffer
	id   constants.ID
}

func (x *Ctx) Tr(k []byte) uint64 {
	return x.tr(x.id, k)
}

func (x *Ctx) Or(o *Ctx) {
	x.o.Or(&o.o)
	x.omit.Or(&o.omit)
}

func (x *Ctx) ToArray() []uint64 {
	return x.o.ToArray()
}

func (x *Ctx) Bitmap() *roaring64.Bitmap {
	return x.o.Clone()
}

func New(id constants.ID, tr blob.Func) *Ctx {
	return &Ctx{}
}

func (x *Ctx) Reset() {
	x.o.Clear()
	x.omit.Clear()
	x.buf.Reset()
}

func (x *Ctx) FST() *roaring64.Bitmap {
	if x.omit.IsEmpty() {
		return x.o.Clone()
	}
	clone := x.o.Clone()
	clone.AndNot(&x.omit)
	return clone
}

func (x *Ctx) Attr(prefix string) func(key string, value pcommon.Value) bool {
	return func(key string, value pcommon.Value) bool {
		return x.SetAttribute(prefix+key, value)
	}
}

func (x *Ctx) Omit(u uint64) {
	x.omit.Add(u)
}

func (x *Ctx) SetAttribute(key string, value pcommon.Value) bool {
	id := x.Set(key, value.AsString())
	switch value.Type() {
	case pcommon.ValueTypeBytes, pcommon.ValueTypeSlice, pcommon.ValueTypeMap:
		x.omit.Add(id)
	}
	return true
}

func (x *Ctx) Set(key, value string) uint64 {
	x.buf.Reset()
	x.buf.WriteString(key)
	x.buf.WriteByte('=')
	x.buf.WriteString(value)
	id := x.tr(x.id, x.buf.Bytes())
	x.o.Add(id)
	return id
}
