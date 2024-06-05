package ernestdb

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/blevesearch/vellum"
	re "github.com/blevesearch/vellum/regexp"
	"github.com/cespare/xxhash/v2"
	"github.com/gernest/ernestdb/keys"
	"github.com/gernest/ernestdb/shardwidth"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

func Queryable(db Store, mints, maxts int64) {

	// adjust to epoch
	mints = max(mints, epochMs)
	if maxts < mints {
		maxts = mints
	}
	if maxts == epochMs {
		return
	}
	minShard := (mints - epochMs) / shardwidth.ShardWidth
	maxShard := (maxts - epochMs) / shardwidth.ShardWidth

	if minShard == maxShard {

	}
}

type singleQuerier struct {
	storage.LabelQuerier
	shard uint64
	db    Store
}

var _ storage.Querier = (*singleQuerier)(nil)

func (s *singleQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	return nil
}

func Match(db Store, shard uint64, matchers ...*labels.Matcher) error {
	exist, err := readBitmap(db, (&keys.Exists{ShardID: shard}).Key())
	if err != nil {
		return err
	}
	if len(matchers) == 0 {
		// match all series
		it := exist.Iterator()
		for it.HasNext() {
			// Read series
		}
		return nil
	}

	var (
		hasRe bool
	)

	allLabels, err := readBitmap(db, (&keys.FSTBitmap{ShardID: shard}).Key())
	if err != nil {
		return err
	}
	lbl := roaring64.New()
	var buf bytes.Buffer
	var h xxhash.Digest

	for _, m := range matchers {
		switch m.Type {
		case labels.MatchEqual, labels.MatchNotEqual:
			buf.Reset()
			buf.WriteString(m.Name)
			buf.WriteByte('=')
			buf.WriteString(m.Value)
			h.Reset()
			h.Write(buf.Bytes())
			label := h.Sum64()
			if !allLabels.Contains(label) {
				// Return early, this matcher will never satisfy.
				return nil
			}
			if m.Type == labels.MatchEqual {
				lbl.Add(label)
			} else {
				all := allLabels.Clone()
				all.Remove(label)
				lbl.Or(all)
			}
		default:
			hasRe = true
		}
	}
	if hasRe {
		err := readFST(db, shard, func(fst *vellum.FST) error {
			o := roaring64.New()
			for _, m := range matchers {
				switch m.Type {
				case labels.MatchRegexp, labels.MatchNotRegexp:
					rx, err := compile(&buf, m.Name, m.Value)
					if err != nil {
						return fmt.Errorf("compiling matcher %q %w", m.String(), err)
					}
					o.Clear()
					itr, err := fst.Search(rx, nil, nil)
					for err == nil {
						_, value := itr.Current()
						o.Add(value)
						err = itr.Next()
					}
					if m.Type == labels.MatchRegexp {
						lbl.Or(o)
					} else {
						all := allLabels.Clone()
						all.AndNot(o)
						lbl.Or(all)
					}
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	// lbl contains all labels satisfying the matchers conditions. The series we
	// want is the intersection of all labels
	return nil
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

func readFST(db Store, shard uint64, f func(fst *vellum.FST) error) error {
	return db.Get((&keys.FST{ShardID: shard}).Key(), func(val []byte) error {
		fst, err := vellum.Load(val)
		if err != nil {
			return err
		}
		return f(fst)
	})
}

func readBSI(db Store, key []byte) (*roaring64.BSI, error) {
	o := roaring64.NewDefaultBSI()
	err := db.Get(key, func(val []byte) error {
		_, err := o.ReadFrom(bytes.NewReader(val))
		return err
	})
	if err != nil {
		return nil, err
	}
	return o, nil
}

func readBitmap(db Store, key []byte) (*roaring64.Bitmap, error) {
	o := roaring64.New()
	err := db.Get(key, o.UnmarshalBinary)
	if err != nil {
		return nil, err
	}
	return o, nil
}
