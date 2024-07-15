package store

import (
	"bytes"

	"github.com/RoaringBitmap/roaring"
	"github.com/gernest/rbf/dsl/bsi"
	"github.com/gernest/rbf/dsl/cursor"
	rroaring "github.com/gernest/roaring"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
)

func (db *DB) Exemplars(min, max int64, matchers ...[]*labels.Matcher) ([]exemplar.QueryResult, error) {
	a := roaring.New()
	for i := range matchers {
		m := roaring.New()
		err := db.Match(m, matchers[i]...)
		if err != nil {
			return nil, err
		}
		a.Or(m)
	}

	if a.IsEmpty() {
		return []exemplar.QueryResult{}, nil
	}

	txn, err := db.idx.Begin(false)
	if err != nil {
		return nil, err
	}
	defer txn.Rollback()

	tx, err := db.db.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blobs := tx.Bucket(blobBucket)
	if blobs == nil {
		return nil, err
	}

	var blobBuf [4]byte

	it := db.shards.Iterator()
	match := roaring.New()
	ex := &prompb.Exemplar{}
	ms := make(map[uint64]*Ex)

	for it.HasNext() {
		shard := uint64(it.Next())

		txc, err := txn.Cursor(field("timestamp", shard))
		if err != nil {
			txc.Close()
			return nil, err
		}
		// check if we have valid time range
		r, err := bsi.Compare(txc, shard, bsi.RANGE, min, max, nil)
		if err != nil {
			txc.Close()
			return nil, err
		}
		if r.IsEmpty() {
			txc.Close()
			continue
		}

		// we have matches for this shard find  all series within this time range.
		sxc, err := txn.Cursor(field("series", shard))
		if err != nil {
			txc.Close()
			return nil, err
		}

		match.Clear()
		err = Distinct(sxc, match, r)
		if err != nil {
			sxc.Close()
			txc.Close()
			return nil, err
		}
		match.And(a)

		if match.IsEmpty() {
			sxc.Close()
			txc.Close()
			continue
		}

		sit := match.Iterator()
		lxc, err := txn.Cursor(field("labels", shard))
		if err != nil {
			sxc.Close()
			txc.Close()
			return nil, err
		}
		exc, err := txn.Cursor(field("exemplars", shard))
		if err != nil {
			sxc.Close()
			txc.Close()
			lxc.Close()
			return nil, err
		}

		for sit.HasNext() {
			series := uint64(sit.Next())

			f, err := cursor.Row(sxc, shard, series)
			if err != nil {
				txc.Close()
				sxc.Close()
				lxc.Close()
				exc.Close()
				return nil, err
			}

			// f matches through the whole shard, we need to reduce to what is in the
			// current time range.
			f = f.Intersect(r)

			columns := f.Columns()

			sx, ok := ms[series]
			if !ok {
				// read labels: only one column is enough
				sx = &Ex{}
				err = cursor.Rows(lxc, shard,
					func(row uint64) error {
						name, value, _ := bytes.Cut(get(blobs, &blobBuf, uint32(row)), sep)
						sx.Labels = append(sx.Labels, labels.Label{
							Name:  string(name),
							Value: string(value),
						})
						return nil
					},
					rroaring.NewBitmapColumnFilter(columns[0]),
				)
				if err != nil {
					txc.Close()
					sxc.Close()
					lxc.Close()
					exc.Close()
					return nil, err
				}
				ms[series] = sx
			}
			err = cursor.Rows(exc, shard,
				func(row uint64) error {
					sx.Ex.Add(uint32(row))
					return nil
				},
				rroaring.NewBitmapColumnFilter(columns[0]),
			)
			if err != nil {
				txc.Close()
				sxc.Close()
				lxc.Close()
				exc.Close()
				return nil, err
			}
		}
		txc.Close()
		sxc.Close()
		lxc.Close()
		exc.Close()
	}
	result := make([]exemplar.QueryResult, 0, len(ms))
	for _, e := range ms {
		if e.Ex.IsEmpty() {
			continue
		}
		o := make([]exemplar.Exemplar, 0, e.Ex.GetCardinality())
		it := e.Ex.Iterator()
		for it.HasNext() {
			ex.Reset()
			ex.Unmarshal(get(blobs, &blobBuf, it.Next()))
			o = append(o, decodeExemplar(ex))
		}
		result = append(result, exemplar.QueryResult{
			SeriesLabels: e.Labels,
			Exemplars:    o,
		})
	}
	return result, nil
}

type Ex struct {
	Labels labels.Labels
	Ex     roaring.Bitmap
}

func decodeExemplar(ex *prompb.Exemplar) exemplar.Exemplar {
	o := exemplar.Exemplar{
		Value: ex.Value,
		Ts:    ex.Timestamp,
	}
	if len(ex.Labels) > 0 {
		o.Labels = make(labels.Labels, len(ex.Labels))
		for i := range ex.Labels {
			o.Labels[i] = labels.Label{
				Name:  ex.Labels[i].Name,
				Value: ex.Labels[i].Value,
			}
		}
	}
	return o
}
