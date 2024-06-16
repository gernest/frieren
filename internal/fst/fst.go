package fst

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/blevesearch/vellum"
	re "github.com/blevesearch/vellum/regexp"
	"github.com/dgraph-io/badger/v4"
	"github.com/gernest/frieren/internal/blob"
	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/frieren/internal/keys"
	"github.com/gernest/frieren/internal/store"
	"github.com/gernest/rbf"
	"github.com/prometheus/prometheus/model/labels"
)

// Match returns label IDs that match all matchers.
func Match(txn *badger.Txn, find blob.Find, shard uint64, view string, id constants.ID, matchers ...*labels.Matcher) (result []uint64, err error) {
	var buf bytes.Buffer
	err = Read(txn, shard, view, id, func(fst *vellum.FST) error {
		equal := roaring64.New()
		notEqual := roaring64.New()
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
						equal.Add(value)
					} else {
						notEqual.Add(value)
					}
				}
			case labels.MatchEqual, labels.MatchNotEqual:
				buf.Reset()
				buf.WriteString(m.Name)
				buf.WriteByte('=')
				buf.WriteString(m.Value)
				value, ok := find(id, buf.Bytes())
				if !ok {
					return nil
				}
				if m.Type == labels.MatchEqual {
					equal.Add(value)
				} else {
					notEqual.Add(value)
				}
			}
		}
		if !notEqual.IsEmpty() {
			if !equal.IsEmpty() {
				equal.AndNot(notEqual)
			} else {
				// We only negation matchers
				err = store.Get(txn, keys.FSTBitmap(&buf, id, shard, view), equal.UnmarshalBinary)
				if err != nil {
					return fmt.Errorf("reading fst bitmap %w", err)
				}
				equal.AndNot(notEqual)
			}
		}
		result = equal.ToArray()
		return nil
	})
	return
}

func MatchSet(txn *badger.Txn, tx *rbf.Tx, shard uint64, view string, id constants.ID, matchers ...[]*labels.Matcher) (result []uint64, err error) {
	var buf bytes.Buffer
	err = Read(txn, shard, view, id, func(fst *vellum.FST) error {

		all := roaring64.New()

		equal := roaring64.New()
		notEqual := roaring64.New()
		rs := roaring64.New()
		var readAll bool

		for _, ms := range matchers {
			equal.Clear()
			notEqual.Clear()
			for _, m := range ms {
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
							equal.Add(value)
						} else {
							notEqual.Add(value)
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
						return nil
					}
					if m.Type == labels.MatchEqual {
						equal.Add(value)
					} else {
						notEqual.Add(value)
					}
				}
			}
			if !notEqual.IsEmpty() {
				if !equal.IsEmpty() {
					equal.AndNot(notEqual)
				} else {
					if !readAll {
						// We only negation matchers
						err = store.Get(txn, keys.FSTBitmap(&buf, id, shard, view), equal.UnmarshalBinary)
						if err != nil {
							return fmt.Errorf("reading fst bitmap %w", err)
						}
						readAll = true
					}
					equal.Or(all)
					equal.AndNot(notEqual)
				}
			}
			rs.Or(equal)
		}
		result = rs.ToArray()
		return nil
	})
	return
}

var eql = []byte("=")

func Labels(txn *badger.Txn, shard uint64, view string, id constants.ID, f func(name, value []byte)) error {
	return Read(txn, shard, view, id, func(fst *vellum.FST) error {
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

func LabelNames(txn *badger.Txn, shard uint64, view string, id constants.ID, name string, f func(name, value []byte)) error {
	rx, err := re.New(name + "=.*")
	if err != nil {
		return err
	}
	return Read(txn, shard, view, id, func(fst *vellum.FST) error {
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
func Read(txn *badger.Txn, shard uint64, view string, id constants.ID, f func(fst *vellum.FST) error) error {
	key := keys.FST(new(bytes.Buffer), id, shard, view)
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
