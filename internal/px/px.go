package px

import (
	"bytes"
	"crypto/sha512"
	"slices"

	v1 "github.com/gernest/frieren/gen/go/fri/v1"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"google.golang.org/protobuf/proto"
)

type Ctx struct {
	buf    bytes.Buffer
	labels map[string]struct{}
}

func New() *Ctx {
	return &Ctx{labels: make(map[string]struct{})}
}

func (x *Ctx) Or(o *Ctx) {
	for k := range o.labels {
		x.labels[k] = struct{}{}
	}
}

func (x *Ctx) Sum() ([]byte, []string) {
	h := sha512.New512_224()
	o := make([]string, 0, len(x.labels))
	for k := range x.labels {
		h.Write([]byte(k))
		o = append(o, k)
	}
	slices.Sort(o)
	return h.Sum(nil), o
}

func (x *Ctx) Metadata() []byte {
	o := make([]string, 0, len(x.labels))
	for k := range x.labels {
		o = append(o, k)
	}
	slices.Sort(o)
	data, _ := proto.Marshal(&v1.Entry_StructureMetadata{
		Labels: o,
	})
	return data
}
func (x *Ctx) Labels() []string {
	o := make([]string, 0, len(x.labels))
	for k := range x.labels {
		o = append(o, k)
	}
	slices.Sort(o)
	return o
}

func (x *Ctx) Reset() {
	x.buf.Reset()
	clear(x.labels)
}

func (x *Ctx) SetAttribute(key string, value pcommon.Value) bool {
	x.Set(key, value.AsString())
	return true
}

func (x *Ctx) Set(key, value string) {
	x.buf.Reset()
	x.buf.WriteString(key)
	x.buf.WriteByte('=')
	x.buf.WriteString(value)
	x.labels[x.buf.String()] = struct{}{}
}
