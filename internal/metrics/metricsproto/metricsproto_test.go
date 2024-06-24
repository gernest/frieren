package metricsproto

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"slices"
	"testing"

	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/frieren/internal/store"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"
)

func TestFrom(t *testing.T) {
	db, err := store.New(t.TempDir())
	require.NoError(t, err)
	defer db.Close()
	var m SeriesMap
	view := "test"
	o := new(bytes.Buffer)

	err = db.Update(func(tx *store.Tx) error {
		vx := tx.View(nil, view)
		m = From(Sample(), vx)
		defer m.Release()
		return m.Serialize(o, vx)
	})
	require.NoError(t, err)
	// err = os.WriteFile("testdata/series", o.Bytes(), 0600)
	// require.NoError(t, err)
	data, err := os.ReadFile("testdata/series")
	require.NoError(t, err)
	require.Equal(t, string(data), o.String())
}

func (m SeriesMap) Serialize(b *bytes.Buffer, tr *store.View) error {
	b.Reset()
	keys := make([]uint64, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	ms := jsonpb.Marshaler{Indent: " "}
	ts := &prompb.TimeSeries{}
	for i := range keys {
		if i != 0 {
			b.WriteByte('\n')
		}
		fmt.Fprintln(b, keys[i])
		err := m[keys[i]].To(ts, tr)
		if err != nil {
			return err
		}
		err = ms.Marshal(b, ts)
		if err != nil {
			return err
		}
	}
	return nil
}

var sep = []byte("=")

func (s *Series) To(o *prompb.TimeSeries, tr *store.View) error {
	o.Reset()
	if len(s.Labels) > 0 {
		o.Labels = make([]prompb.Label, len(s.Labels))
		for i := range s.Labels {
			v := tr.Tr(constants.MetricsLabels, s.Labels[i])
			key, value, _ := bytes.Cut(v, sep)
			o.Labels[i] = prompb.Label{
				Name:  string(key),
				Value: string(value),
			}
		}
	}

	if len(s.Exemplars) > 0 {
		o.Exemplars = make([]prompb.Exemplar, len(s.Exemplars))
		for i := range o.Exemplars {
			e := &o.Exemplars[i]
			err := e.Unmarshal(tr.Tr(constants.MetricsExemplars, s.Exemplars[i]))
			if err != nil {
				return err
			}
		}
	}

	if len(s.Histograms) > 0 {
		o.Histograms = make([]prompb.Histogram, len(s.Histograms))
		for i := range o.Histograms {
			h := &o.Histograms[i]
			err := h.Unmarshal(tr.Tr(constants.MetricsHistogram, s.Histograms[i]))
			if err != nil {
				return err
			}
		}
	}

	if len(s.Values) > 0 {
		o.Samples = make([]prompb.Sample, len(s.Values))
		for i := range o.Samples {
			o.Samples[i] = prompb.Sample{
				Timestamp: int64(s.Timestamp[i]),
				Value:     math.Float64frombits(s.Values[i]),
			}
		}
	}

	return nil
}
