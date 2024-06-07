package ernestdb

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/blevesearch/vellum"
	re "github.com/blevesearch/vellum/regexp"
	"github.com/cespare/xxhash/v2"
	"github.com/dgraph-io/badger/v4"
	"github.com/gernest/ernestdb/keys"
	"github.com/gernest/rbf"
	"github.com/gernest/rbf/quantum"
	"github.com/gernest/rows"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/util/annotations"
)

type Queryable struct {
	db  *badger.DB
	idx *rbf.DB
}

var _ storage.Queryable = (*Queryable)(nil)

func (q *Queryable) Querier(mints, maxts int64) (storage.Querier, error) {
	var views []string
	if date(mints).Equal(date(maxts)) {
		// Same day generate a single view
		views = []string{quantum.ViewByTimeUnit("", time.UnixMilli(mints), 'D')}
	} else {
		// We wantview that might contain maxts to be included too, we need to add
		// extra date
		views = quantum.ViewsByTimeRange("",
			time.UnixMilli(mints), time.UnixMilli(maxts).AddDate(0, 0, 1),
			quantum.TimeQuantum("D"))
	}
	vs := make(map[int]*roaring64.Bitmap)
	tx, err := q.idx.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	all := roaring64.New()
	for i := range views {
		// read the shards observed per view
		r, err := row(0, viewFor("metrics.shards", views[i], 0), tx, 0)
		if err != nil {
			return nil, fmt.Errorf("reading shards %w", err)
		}
		if r.IsEmpty() {
			continue
		}
		b := roaring64.New()
		b.AddMany(r.Columns())
		vs[i] = b
		all.Or(b)
	}
	shards := all.ToArray()
	shardViews := make([][]string, len(shards))
	for i := range shards {
		for k, v := range vs {
			if v.Contains(shards[i]) {
				shardViews[i] = append(shardViews[i], views[k])
			}
		}
	}
	return &Querier{
		shards: shards,
		views:  shardViews,
		db:     q.db,
		idx:    q.idx,
	}, nil

}

func date(ts int64) time.Time {
	y, m, d := time.UnixMilli(ts).Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

type Querier struct {
	storage.LabelQuerier
	shards []uint64
	views  [][]string
	db     *badger.DB
	idx    *rbf.DB
}

var _ storage.Querier = (*Querier)(nil)

func (s *Querier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	if len(matchers) == 0 {
		return storage.EmptySeriesSet()
	}
	tx, err := s.idx.Begin(false)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}
	defer tx.Rollback()

	txn := s.db.NewTransaction(false)
	defer txn.Discard()

	yes, no, re := match00(matchers...)
	m := make(MapSet)

	for i, shard := range s.shards {
		a, b := yes, no
		if len(re) > 0 {
			// Regular expressions are applied per shard because we keep a FST for each
			// shard regardless of the view.
			//
			// FST can get quiet big for high cardinality data. By keeping one per shard
			// we make sure we have manageable fst for the expected shard width. FST
			// checks are on hot path it is reasonable to incur the small size penalty
			// for faster queries.
			//
			// we are cloning yes an no bitmaps because we update them in place
			a, b = yes.Clone(), no.Clone()
			err := match01(txn, shard, yes, no, re...)
			if err != nil {
				return storage.ErrSeriesSet(err)
			}
		}
		for _, view := range s.views[i] {
			r := rows.NewRow()
			if !a.IsEmpty() {
				start := true
				it := a.Iterator()
				for it.HasNext() {
					label := it.Next()
					rw, err := EqSet(shard, "metrics.labels", view, tx, label)
					if err != nil {
						err = fmt.Errorf("reading labels %w", err)
						return storage.ErrSeriesSet(err)
					}
					if start {
						r = rw
						start = false
					} else {
						r = r.Intersect(rw)
					}
					if r.IsEmpty() {
						return storage.EmptySeriesSet()
					}
				}
			}
			if !b.IsEmpty() {
				it := b.Iterator()
				exists, err := row(shard, viewFor("metrics.labels", view, shard), tx, 0)
				if err != nil {
					err = fmt.Errorf("reading labels exists bitmap %w", err)
					return storage.ErrSeriesSet(err)
				}
				r = exists
				for it.HasNext() {
					label := it.Next()
					rw, err := EqSet(shard, "metrics.labels", view, tx, label)
					if err != nil {
						err = fmt.Errorf("reading labels %w", err)
						return storage.ErrSeriesSet(err)
					}
					r = r.Difference(rw)
					if r.IsEmpty() {
						return storage.EmptySeriesSet()
					}
				}
			}

			// r is the row ids in this view/shard that we want to read.
			err = m.Build(txn, tx, Translate(txn), hints.Start, hints.End, view, shard, r)
			if err != nil {
				return storage.ErrSeriesSet(err)
			}
		}
	}
	return nil
}

type SeriesSet struct {
	series []storage.Series
	pos    int
}

var _ storage.SeriesSet = (*SeriesSet)(nil)

func (s *SeriesSet) Next() bool {
	s.pos++
	return s.pos < len(s.series)
}

func (s *SeriesSet) At() storage.Series {
	return s.series[s.pos]
}

func (s *SeriesSet) Err() error {
	return nil
}

func (s *SeriesSet) Warnings() annotations.Annotations {
	return nil
}

type MapSet map[uint64]*S

var (
	sep = []byte("=")
)

type Tr func(id uint64, f func([]byte) error) error

func (s MapSet) Build(txn *badger.Txn, tx *rbf.Tx, tr Tr, start, end int64, view string, shard uint64, filter *rows.Row) error {
	add := func(view string, seriesID, shard, validID uint64, samples []chunks.Sample) error {
		sx, ok := s[seriesID]
		if ok {
			sx.Samples = append(sx.Samples, samples...)
			return nil
		}
		lbl, err := ReadSetValue(shard, "metrics.labels", view, tx, validID)
		if err != nil {
			return fmt.Errorf("reading labels %w", err)
		}
		sx = &S{
			Labels:  make(labels.Labels, 0, len(lbl)),
			Samples: samples,
		}
		for i := range lbl {
			err = tr(lbl[i], func(val []byte) error {
				key, value, _ := bytes.Cut(val, sep)
				sx.Labels = append(sx.Labels, labels.Label{
					Name:  string(key),
					Value: string(value),
				})
				return nil
			})
			if err != nil {
				return fmt.Errorf("reading series labels %w", err)
			}
		}
		s[seriesID] = sx
		return nil
	}

	// find matching timestamps
	r, err := Between(shard, "metrics.timestamp", view, tx, uint64(start), uint64(end))
	if err != nil {
		return fmt.Errorf("reading timestamp %w", err)
	}
	if filter != nil {
		r = r.Intersect(filter)
	}
	if r.IsEmpty() {
		return nil
	}

	// find all series
	series, err := TransposeBSI(shard, "metrics.series", view, tx, r)
	if err != nil {
		return err
	}
	if series.IsEmpty() {
		return nil
	}

	// iterate on each series
	it := series.Iterator()

	// Filter to check if series is of histogram type
	kind, err := False(shard, "metrics.kind", view, tx)
	if err != nil {
		return fmt.Errorf("reading kind %w", err)
	}
	mapping := map[uint64]int{}
	for it.HasNext() {
		seriesID := it.Next()
		sr, err := EqBSI(shard, "metrics.series", view, tx, seriesID)
		if err != nil {
			return fmt.Errorf("reading columns for series %w", err)
		}
		sr = sr.Intersect(r)
		if sr.IsEmpty() {
			continue
		}
		columns := sr.Columns()
		clear(mapping)
		for i := range columns {
			mapping[columns[i]] = i
		}

		chunks := make([]chunks.Sample, len(columns))

		if !kind.Includes(columns[0]) {
			// This is a float series
			for i := range chunks {
				chunks[i] = &V{}
			}
			err := extractBSI(shard, viewFor("metrics.value", view, shard), tx, sr, mapping, func(i int, v uint64) error {
				chunks[i].(*V).f = math.Float64frombits(v)
				return nil
			})
			if err != nil {
				return fmt.Errorf("extracting values %w", err)
			}
			err = extractBSI(shard, viewFor("metrics.timestamp", view, shard), tx, sr, mapping, func(i int, v uint64) error {
				chunks[i].(*V).ts = int64(v)
				return nil
			})
			if err != nil {
				return fmt.Errorf("extracting timestamp %w", err)
			}
		} else {
			isFloat := false
			first := true
			hs := &prompb.Histogram{}
			err := extractBSI(shard, viewFor("metrics.value", view, shard), tx, sr, mapping, func(i int, v uint64) error {
				hs.Reset()
				err := tr(v, hs.Unmarshal)
				if err != nil {
					return fmt.Errorf("reading histogram blob %w", err)
				}
				if first {
					_, isFloat = hs.Count.(*prompb.Histogram_CountFloat)
					first = false
				}
				if isFloat {
					chunks[i] = NewFH(hs)
					return nil
				}
				chunks[i] = NewH(hs)
				return nil
			})
			if err != nil {
				return fmt.Errorf("extracting values %w", err)
			}
		}
		err = add(view, seriesID, shard, columns[0], chunks)
		if err != nil {
			return err
		}
	}
	return nil
}

type S struct {
	Labels  labels.Labels
	Samples []chunks.Sample
}

type V struct {
	ts int64
	f  float64
}

var _ chunks.Sample = (*V)(nil)

func (h *V) T() int64                      { return h.ts }
func (h *V) F() float64                    { return h.f }
func (h *V) H() *histogram.Histogram       { return nil }
func (h *V) FH() *histogram.FloatHistogram { return nil }
func (h *V) Type() chunkenc.ValueType      { return chunkenc.ValFloat }

type H struct {
	ts int64
	h  *histogram.Histogram
}

var _ chunks.Sample = (*H)(nil)

func (h *H) T() int64                      { return h.ts }
func (h *H) F() float64                    { return 0 }
func (h *H) H() *histogram.Histogram       { return h.h }
func (h *H) FH() *histogram.FloatHistogram { return nil }
func (h *H) Type() chunkenc.ValueType      { return chunkenc.ValHistogram }

func NewH(o *prompb.Histogram) *H {
	return &H{
		ts: o.Timestamp,
		h:  remote.HistogramProtoToHistogram(*o),
	}
}

type FH struct {
	ts int64
	h  *histogram.FloatHistogram
}

var _ chunks.Sample = (*FH)(nil)

func (h *FH) T() int64                      { return h.ts }
func (h *FH) F() float64                    { return 0 }
func (h *FH) H() *histogram.Histogram       { return nil }
func (h *FH) FH() *histogram.FloatHistogram { return h.h }
func (h *FH) Type() chunkenc.ValueType      { return chunkenc.ValHistogram }

func NewFH(o *prompb.Histogram) *FH {
	return &FH{
		ts: o.Timestamp,
		h:  remote.FloatHistogramProtoToFloatHistogram(*o),
	}
}

func match00(matchers ...*labels.Matcher) (yes, no *roaring64.Bitmap, regex []*labels.Matcher) {
	yes = roaring64.New()
	no = roaring64.New()
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

			if m.Type == labels.MatchEqual {
				yes.Add(label)
			} else {
				no.Add(label)
			}
		default:
			regex = append(regex, m)
		}
	}
	return
}

func match01(txn *badger.Txn, shard uint64, yes, no *roaring64.Bitmap, matchers ...*labels.Matcher) error {
	var buf bytes.Buffer
	return readFST(txn, shard, func(fst *vellum.FST) error {
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
			}
		}
		return nil
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

func readFST(txn *badger.Txn, shard uint64, f func(fst *vellum.FST) error) error {
	return Get(txn, (&keys.FST{ShardID: shard}).Key(), func(val []byte) error {
		fst, err := vellum.Load(val)
		if err != nil {
			return err
		}
		return f(fst)
	})
}
