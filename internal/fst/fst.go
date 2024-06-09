package fst

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/blevesearch/vellum"
	re "github.com/blevesearch/vellum/regexp"
	"github.com/dgraph-io/badger/v4"
	"github.com/gernest/frieren/internal/fields"
	"github.com/gernest/frieren/internal/store"
	"github.com/gernest/rbf"
	"github.com/gernest/roaring"
	"github.com/prometheus/prometheus/model/labels"
)

// Match returns label IDs that match all matchers.
func Match(txn *badger.Txn, tx *rbf.Tx, fra *fields.Fragment, matchers ...*labels.Matcher) (result []uint64, err error) {
	var buf bytes.Buffer
	err = Read(txn, []byte(fra.String()), func(fst *vellum.FST) error {
		all, err := tx.RoaringBitmap((&fields.Fragment{ID: fields.MetricsFSTBitmap, Shard: fra.Shard, View: fra.View}).String())
		if err != nil {
			return fmt.Errorf("reading fst bitmap %w", err)
		}
		r := roaring.NewBitmap()
		for _, m := range matchers {
			switch m.Type {
			case labels.MatchRegexp, labels.MatchNotRegexp:
				rx, err := compile(&buf, m.Name, m.Value)
				if err != nil {
					return fmt.Errorf("compiling matcher %q %w", m.String(), err)
				}
				b := roaring.NewBitmap()
				itr, err := fst.Search(rx, nil, nil)
				for err == nil {
					_, value := itr.Current()
					b.Add(value)
					err = itr.Next()
				}
				if m.Type == labels.MatchRegexp {
					if r.Count() == 0 {
						r = b
					} else {
						r = r.Intersect(b)
					}
				} else {
					clone := all.Clone().Difference(b)
					if r.Count() == 0 {
						r = clone
					} else {
						r = r.Intersect(clone)
					}
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
					return nil
				}
				if m.Type == labels.MatchEqual {
					if !all.Contains(value) {
						return nil
					}
					if r.Count() == 0 {
						r.Add(value)
					} else {
						r = r.Intersect(roaring.NewBitmap(value))
					}
				} else {
					clone := all.Clone()
					clone.Remove(value)
					if r.Count() == 0 {
						r = clone
					} else {
						r = r.Intersect(clone)
					}
				}
			}
			if r.Count() == 0 {
				return nil
			}
		}
		result = r.Slice()
		return nil
	})
	return
}

var eql = []byte("=")

func Labels(txn *badger.Txn, key []byte, f func(name, value []byte)) error {
	return Read(txn, key, func(fst *vellum.FST) error {
		it, err := fst.Iterator(nil, nil)
		for err == nil {
			current, _ := it.Current()
			name, value, _ := bytes.Cut(current, eql)
			f(name, value)
			err = it.Next()
		}
		return nil
	})
}

func LabelNames(txn *badger.Txn, key []byte, name string, f func(name, value []byte)) error {
	rx, err := re.New(name + "=.*")
	if err != nil {
		return err
	}
	return Read(txn, key, func(fst *vellum.FST) error {
		it, err := fst.Search(rx, nil, nil)
		for err == nil {
			current, _ := it.Current()
			name, value, _ := bytes.Cut(current, eql)
			f(name, value)
			err = it.Next()
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
