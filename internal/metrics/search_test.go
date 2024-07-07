package metrics

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/gernest/frieren/internal/util"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"
)

func TestSelect(t *testing.T) {
	db, err := New(t.TempDir())
	require.NoError(t, err)
	defer db.Close()

	m := generateOTLPWriteRequest()

	require.NoError(t, db.Save(m.Metrics()))

	q := db.Queryable()
	t.Run("all metrics", func(t *testing.T) {
		ts := util.TS()
		ms := ts.UnixMilli()

		qry, err := q.Querier(ms, ms)
		require.NoError(t, err)
		set := qry.Select(context.TODO(), false, &storage.SelectHints{
			Start: ms, End: ms,
		}, labels.MustNewMatcher(labels.MatchEqual, "service_name", "test-service"))
		require.NoError(t, set.Err())

		data, err := json.MarshalIndent(matrix(set), "", "  ")
		require.NoError(t, err)
		err = os.WriteFile("testdata/search.json", data, 0600)
		require.NoError(t, err)
	})

}

func matrix(ss storage.SeriesSet) (o promql.Matrix) {
	for ss.Next() {
		o = append(o, promSeries(ss.At()))
	}
	return
}

func promSeries(s storage.Series) promql.Series {
	o := promql.Series{
		Metric: s.Labels(),
	}
	it := s.Iterator(nil)
	for v := it.Next(); v != chunkenc.ValNone; v = it.Next() {
		switch v {
		case chunkenc.ValFloat:
			v, f := it.At()
			o.Floats = append(o.Floats, promql.FPoint{
				T: v,
				F: f,
			})
		case chunkenc.ValFloatHistogram:
			v, f := it.AtFloatHistogram(nil)
			o.Histograms = append(o.Histograms, promql.HPoint{
				T: v, H: f,
			})
		case chunkenc.ValHistogram:
			v, f := it.AtHistogram(nil)
			o.Histograms = append(o.Histograms, promql.HPoint{
				T: v, H: f.ToFloat(nil),
			})
		}
	}
	return o
}
