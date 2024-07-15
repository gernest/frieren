package store

import (
	"slices"
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/stretchr/testify/require"
)

func TestWrite(t *testing.T) {

	sample := []struct {
		key, value string
	}{
		{"__name__", "foo"},
		{"__name__", "bar"},
		{"key", "value"},
	}
	db, err := New(t.TempDir())
	require.NoError(t, err)
	defer db.Close()

	b := NewBatch()
	lbl, series := b.Write(func(w Labels) {
		for _, v := range sample {
			w.Write([]byte(v.key), []byte(v.value))
		}
	})
	err = db.apply(b)
	require.NoError(t, err)

	names, err := db.Names()
	require.NoError(t, err)
	require.Equal(t, []string{"__name__", "key"}, names)

	values, err := db.Values("__name__")
	require.NoError(t, err)
	require.Equal(t, []string{"bar", "foo"}, values)
	var blobs []string

	err = db.Get(lbl, func(pos int, value []byte) error {
		blobs = append(blobs, string(value))
		return nil
	})
	require.NoError(t, err)
	slices.Sort(blobs)
	require.Equal(t, []string{"__name__=bar", "__name__=foo", "key=value"}, blobs)

	o := roaring.New()
	err = db.Match(o, labels.MustNewMatcher(
		labels.MatchEqual, "__name__", "foo",
	))
	require.NoError(t, err)
	require.Equal(t, []uint32{series}, o.ToArray())

	o.Clear()
	err = db.Match(o, labels.MustNewMatcher(
		labels.MatchRegexp, "__name__", "foo",
	))
	require.NoError(t, err)
	require.Equal(t, []uint32{series}, o.ToArray())
}

func TestSave(t *testing.T) {
	db, err := New(t.TempDir())
	require.NoError(t, err)
	defer db.Close()

	m := generateOTLPWriteRequest().Metrics()
	err = db.Save(m)
	require.NoError(t, err)
	meta, err := db.Metadata("")
	require.NoError(t, err)
	want := map[string][]metadata.Metadata{
		"test_counter_total": {
			{Type: model.MetricTypeCounter, Unit: "", Help: "test-counter-description"},
		},
		"test_exponential_histogram": {
			{Type: model.MetricTypeHistogram, Unit: "", Help: "test-exponential-histogram-description"},
		},
		"test_gauge": {
			{Type: model.MetricTypeGauge, Unit: "", Help: "test-gauge-description"},
		},
		"test_histogram": {
			{Type: model.MetricTypeHistogram, Unit: "", Help: "test-histogram-description"},
		},
	}
	require.Equal(t, want, meta)

	meta, err = db.Metadata("test_histogram")
	require.NoError(t, err)

	want = map[string][]metadata.Metadata{
		"test_histogram": {
			{Type: model.MetricTypeHistogram, Unit: "", Help: "test-histogram-description"},
		},
	}
	require.Equal(t, want, meta)
}
