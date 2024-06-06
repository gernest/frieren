package ernestdb

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/blevesearch/vellum"
	re "github.com/blevesearch/vellum/regexp"
	"github.com/cespare/xxhash/v2"
	"github.com/gernest/ernestdb/keys"
	"github.com/gernest/ernestdb/shardwidth"
	"github.com/gernest/rbf"
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
	db Store
}

var _ storage.Queryable = (*Queryable)(nil)

func (q *Queryable) Querier(mints, maxts int64) (storage.Querier, error) {

	// adjust to epoch
	mints = max(mints, epochMs)
	if maxts < mints {
		maxts = mints
	}
	if maxts == epochMs {
		return &Querier{}, nil
	}

	minShard := (mints - epochMs) / shardwidth.ShardWidth
	maxShard := (maxts - epochMs) / shardwidth.ShardWidth

	shards, err := readBitmap(q.db, keys.Shards{}.Key())
	if err != nil {
		return nil, err
	}
	if minShard == maxShard {
		if !shards.Contains(uint64(minShard)) {
			return &Querier{}, nil
		}
		return &Querier{shards: []uint64{uint64(minShard)}}, nil
	}
	b := roaring64.New()
	b.AddRange(uint64(minShard), uint64(maxShard))
	shards.And(b)
	if shards.IsEmpty() {
		return &Querier{}, nil
	}
	return &Querier{shards: shards.ToArray()}, nil
}

type Querier struct {
	storage.LabelQuerier
	shards []uint64
	views  [][]string
	db     Store
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
			err := match01(s.db, shard, yes, no, re...)
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
			err = m.Build(s.db, tx, hints.Start, hints.End, view, shard, r)
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

func (s MapSet) Build(db Store, tx *rbf.Tx, start, end int64, view string, shard uint64, filter *rows.Row) error {
	blobSlice := (&keys.Blob{}).Slice()
	kb := make([]byte, 0, len(blobSlice)*8)
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
			blobSlice[len(blobSlice)-1] = lbl[i]
			err = db.Get(keys.Encode(kb, blobSlice), func(val []byte) error {
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
		// sr is a set of column ids that belongs to seriesID and matches the filter.
		// We need to determine what kind of series before reading. Only one column
		// id is enough
		var validColumnID uint64
		sr.RangeColumns(func(u uint64) error {
			validColumnID = u
			return io.EOF
		})

		kind, err := MutexValue(shard, "metrics.kind", view, tx, validColumnID)
		if err != nil {
			return fmt.Errorf("reading series kind %w", err)
		}
		chunks := make([]chunks.Sample, 0, sr.Count())
		switch metricsKind(kind) {
		case metricsFloat:
		case metricsHistogram:
		case metricsFloatHistogram:
		}
		err = add(view, seriesID, shard, validColumnID, chunks)
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

func match01(db Store, shard uint64, yes, no *roaring64.Bitmap, matchers ...*labels.Matcher) error {
	var buf bytes.Buffer
	return readFST(db, shard, func(fst *vellum.FST) error {
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

// Series returns a bitmap of all series ID that match matchers for the given shard.
func Series(db Store, shard uint64, matchers ...*labels.Matcher) (yes, no *roaring64.Bitmap, err error) {

	var (
		hasRe bool
	)

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
			hasRe = true
		}
	}
	if hasRe {
		err := readFST(db, shard, func(fst *vellum.FST) error {
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
		if err != nil {
			return nil, nil, err
		}
	}
	return yes, no, nil
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
