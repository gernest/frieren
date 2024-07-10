package metrics

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/blevesearch/vellum"
	v1 "github.com/gernest/frieren/gen/go/fri/v1"
	"github.com/gernest/frieren/internal/lbx"
	"github.com/gernest/rbf/dsl"
	"github.com/gernest/rbf/dsl/bsi"
	"github.com/gernest/rbf/dsl/cursor"
	"github.com/gernest/rbf/dsl/mutex"
	rq "github.com/gernest/rbf/dsl/query"
	"github.com/gernest/rbf/dsl/tx"
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
	store *Store
}

var _ storage.Queryable = (*Queryable)(nil)

func (q *Queryable) Querier(mints, maxts int64) (storage.Querier, error) {
	r, err := q.store.Reader()
	if err != nil {
		return nil, err
	}
	defer r.Release()

	return &Querier{
		store:  q.store,
		shards: r.Range(time.UnixMilli(mints).UTC(), time.UnixMilli(maxts).UTC()),
	}, nil

}

type Querier struct {
	store  *Store
	shards []dsl.Shard
}

var _ storage.Querier = (*Querier)(nil)

func (q *Querier) Close() error {
	return nil
}

func (s *Querier) LabelValues(ctx context.Context, name string, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	r, err := s.store.Reader()
	if err != nil {
		return nil, nil, err
	}
	defer r.Release()

	m := map[string]struct{}{}
	prefix := []byte(name + "=")
	err = r.Tr().Search("labels", &vellum.AlwaysMatch{}, prefix, nil, func(key []byte, value uint64) error {
		if !bytes.HasPrefix(key, prefix) {
			return io.EOF
		}
		m[string(bytes.TrimPrefix(key, prefix))] = struct{}{}
		return nil
	})
	if err != nil {
		if !errors.Is(err, io.EOF) {
			return nil, nil, err
		}
	}
	o := make([]string, 0, len(m))
	for k := range m {
		o = append(o, k)
	}
	slices.Sort(o)
	return o, nil, nil
}

func (s *Querier) LabelNames(ctx context.Context, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	r, err := s.store.Reader()
	if err != nil {
		return nil, nil, err
	}
	defer r.Release()

	m := map[string]struct{}{}
	split := []byte("=")
	err = r.Tr().Search("labels", &vellum.AlwaysMatch{}, nil, nil, func(key []byte, value uint64) error {
		name, _, _ := bytes.Cut(key, split)
		m[string(name)] = struct{}{}
		return nil
	})
	if err != nil {
		if !errors.Is(err, io.EOF) {
			return nil, nil, err
		}
	}
	o := make([]string, 0, len(m))
	for k := range m {
		o = append(o, k)
	}
	slices.Sort(o)
	return o, nil, nil
}

func (s *Querier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	if len(matchers) == 0 || len(s.shards) == 0 {
		return storage.EmptySeriesSet()
	}

	m := make(MapSet)

	// We use timestamp filter as the base filter
	base := bsi.Filter("timestamp", bsi.RANGE, hints.Start, hints.End)

	// all matchers use AND
	filter := make(rq.And, 0, len(matchers))
	var b bytes.Buffer
	for _, m := range matchers {
		var op mutex.OP
		b.Reset()
		b.WriteString(m.Name)
		b.WriteByte('=')
		switch m.Type {
		case labels.MatchEqual:
			op = mutex.EQ
			b.WriteString(m.Value)
		case labels.MatchNotEqual:
			op = mutex.NEQ
			b.WriteString(m.Value)
		case labels.MatchRegexp:
			op = mutex.RE
			b.WriteString(clean(m.Value))
		case labels.MatchNotRegexp:
			op = mutex.NRE
			b.WriteString(clean(m.Value))
		}
		filter = append(filter, &mutex.MatchString{
			Field: "labels",
			Op:    op,
			Value: b.String(),
		})
	}
	r, err := s.store.Reader()
	if err != nil {
		return storage.ErrSeriesSet(err)
	}
	defer r.Release()
	isSeriesQuery := hints.Func == "series"
	for i := range s.shards {

		err := r.View(s.shards[i], func(txn *tx.Tx) error {
			r, err := base.Apply(txn, nil)
			if err != nil {
				return err
			}
			if r.IsEmpty() {
				return nil
			}

			f, err := filter.Apply(txn, r)
			if err != nil {
				return err
			}
			if f.IsEmpty() {
				return nil
			}

			series, err := txn.Tx.Cursor(txn.Key("series"))
			if err != nil {
				return err
			}
			defer series.Close()
			labels, err := txn.Tx.Cursor(txn.Key("labels"))
			if err != nil {
				return err
			}
			defer labels.Close()

			kind, err := txn.Tx.Cursor(txn.Key("kind"))
			if err != nil {
				return err
			}
			defer kind.Close()

			ts, err := txn.Tx.Cursor(txn.Key("timestamp"))
			if err != nil {
				return err
			}
			defer ts.Close()

			vc, err := txn.Tx.Cursor(txn.Key("value"))
			if err != nil {
				return err
			}
			defer vc.Close()

			hc, err := txn.Tx.Cursor(txn.Key("histogram"))
			if err != nil {
				return err
			}
			defer hc.Close()

			floats, err := cursor.Row(kind, txn.Shard, uint64(v1.Sample_FLOAT))
			if err != nil {
				return err
			}

			histograms, err := cursor.Row(kind, txn.Shard, uint64(v1.Sample_HISTOGRAM))
			if err != nil {
				return err
			}
			histograms = histograms.Intersect(f)

			return lbx.Distinct(series, f, txn.Shard, func(value uint64, columns *rows.Row) error {
				cols := columns.Columns()
				sx, ok := m[value]
				if !ok {
					lbl, err := lbx.Labels(labels, "labels", txn, cols[0])
					if err != nil {
						return err
					}
					sx = &S{
						Labels: lbl,
					}
					m[value] = sx
				}
				if isSeriesQuery {
					return nil
				}
				chunks := make([]chunks.Sample, len(cols))
				mapping := map[uint64]int{}
				for i := range cols {
					mapping[cols[i]] = i
				}
				data := lbx.NewData(cols)

				if histograms.Includes(cols[0]) {
					hs := &prompb.Histogram{}
					err = lbx.BSI(data, cols, ts, columns, txn.Shard, func(position int, value int64) error {
						hs.Reset()
						hs.Unmarshal(txn.Tr.Blob("histogram", uint64(value)))
						if _, isFloat := hs.Count.(*prompb.Histogram_CountFloat); isFloat {
							chunks[position] = NewFH(hs)
						} else {
							chunks[position] = NewH(hs)
						}
						return nil
					})
					if err != nil {
						return fmt.Errorf("reading histogram %w", err)
					}
				}
				if floats.Includes(cols[0]) {
					err = lbx.BSI(data, cols, ts, columns, txn.Shard, func(position int, value int64) error {
						chunks[position] = &V{ts: value}
						return nil
					})
					if err != nil {
						return fmt.Errorf("reading timestamp %w", err)
					}
					err = lbx.BSI(data, cols, vc, columns, txn.Shard, func(position int, value int64) error {
						chunks[position].(*V).f = math.Float64frombits(uint64(value))
						return nil
					})
					if err != nil {
						return fmt.Errorf("reading values %w", err)
					}
				}
				sx.Samples = append(sx.Samples, chunks...)
				return nil
			})
		})
		if err != nil {
			return storage.ErrSeriesSet(err)
		}
	}

	return NewSeriesSet(m)
}

func clean(s string) string {
	s = strings.TrimPrefix(s, "^")
	s = strings.TrimSuffix(s, "$")
	return s
}

type SeriesSet struct {
	series []storage.Series
	pos    int
}

func NewSeriesSet(m MapSet) *SeriesSet {
	keys := make([]uint64, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	s := make([]storage.Series, 0, len(m))
	for i := range keys {
		s = append(s, storage.NewListSeries(m[keys[i]].Labels, m[keys[i]].Samples))
	}
	return &SeriesSet{series: s}
}

var _ storage.SeriesSet = (*SeriesSet)(nil)

func (s *SeriesSet) Next() bool {
	return s.pos < len(s.series)
}

func (s *SeriesSet) At() storage.Series {
	defer func() { s.pos++ }()
	return s.series[s.pos]
}

func (s *SeriesSet) Err() error {
	return nil
}

func (s *SeriesSet) Warnings() annotations.Annotations {
	return nil
}

type MapSet map[uint64]*S

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
