package store

import (
	"encoding/json"
	"os"
	"slices"
	"testing"

	"github.com/gernest/frieren/internal/util"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
)

func TestSelect(t *testing.T) {
	db, err := New(t.TempDir())
	require.NoError(t, err)
	defer db.Close()

	m := generateOTLPWriteRequest()

	require.NoError(t, db.Save(m.Metrics()))
	ts := util.TS().UnixMilli()
	rs, err := db.Select(ts, ts, labels.MustNewMatcher(labels.MatchEqual, "job", "test-service"))
	require.NoError(t, err)
	keys := make([]uint64, 0, len(rs))
	for k := range rs {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	mx := make(promql.Matrix, len(keys))
	for i := range keys {
		mx[i] = promSeries(rs[keys[i]])
	}
	data, err := json.MarshalIndent(mx, "", "  ")
	require.NoError(t, err)
	err = os.WriteFile("testdata/search.json", data, 0600)
	require.NoError(t, err)
}

func promSeries(s *S) promql.Series {
	o := promql.Series{
		Metric: s.Labels,
	}
	if len(s.Samples) > 0 {
		switch s.Samples[0].(type) {
		case *V:
			o.Floats = make([]promql.FPoint, len(s.Samples))
			for i := range s.Samples {
				e := s.Samples[i].(*V)
				o.Floats[i] = promql.FPoint{
					T: e.ts, F: e.f,
				}
			}
		case *H:
			o.Histograms = make([]promql.HPoint, len(s.Samples))
			for i := range s.Samples {
				e := s.Samples[i].(*H)
				o.Histograms[i] = promql.HPoint{
					T: e.ts,
					H: e.h.ToFloat(nil),
				}
			}
		}
	}
	return o
}
