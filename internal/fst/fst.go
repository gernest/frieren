package fst

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/blevesearch/vellum"
	re "github.com/blevesearch/vellum/regexp"
	"github.com/dgraph-io/badger/v4"
	"github.com/gernest/frieren/internal/store"
	"github.com/prometheus/prometheus/model/labels"
)

// Match process matchers and builds yes/no bitmaps of label keys. Where yes is
// for labels with equal matchers and no is for labels with **not equal** matchers.
//
// key refers to the fst to load.
func Match(txn *badger.Txn, key []byte, yes, no *roaring64.Bitmap, matchers ...*labels.Matcher) error {
	var buf bytes.Buffer
	return Read(txn, key, func(fst *vellum.FST) error {
		for _, m := range matchers {
			switch m.Type {
			case labels.MatchRegexp, labels.MatchNotRegexp:
				rx, err := compile(&buf, m.Name, m.Value)
				if err != nil {
					return fmt.Errorf("compiling matcher %q %w", m.String(), err)
				}
				itr, err := fst.Search(rx, nil, nil)
				for err == nil {
					_, value := itr.Current()
					if m.Type == labels.MatchRegexp {
						yes.Add(value)
					} else {
						no.Add(value)
					}
					err = itr.Next()
				}
			case labels.MatchEqual, labels.MatchNotEqual:
				buf.Reset()
				buf.WriteString(m.Name)
				buf.WriteByte('=')
				buf.WriteString(m.Value)
				value, ok, err := fst.Get(buf.Bytes())
				if err != nil {
					return fmt.Errorf("get fst value %w", err)
				}
				if !ok {
					yes.Clear()
					no.Clear()
					return nil
				}
				if m.Type == labels.MatchEqual {
					yes.Add(value)
				} else {
					no.Add(value)
				}
			}
		}
		return nil
	})
}

// Read  loads vellum.FST from database and calls f with it.
func Read(txn *badger.Txn, key []byte, f func(fst *vellum.FST) error) error {
	return store.Get(txn, key, func(val []byte) error {
		fst, err := vellum.Load(val)
		if err != nil {
			return err
		}
		defer fst.Close()
		return f(fst)
	})
}

func compile(b *bytes.Buffer, key, value string) (*re.Regexp, error) {
	value = strings.TrimPrefix(value, "^")
	value = strings.TrimSuffix(value, "$")
	b.Reset()
	b.WriteString(key)
	b.WriteByte('=')
	b.WriteString(value)
	return re.New(b.String())
}
