package metrics

import (
	"bytes"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/gernest/frieren/internal/batch"
	"github.com/gernest/frieren/internal/blob"
	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/frieren/internal/fields"
	"github.com/gernest/frieren/internal/store"
	"github.com/gernest/rbf"
	"github.com/gernest/rbf/quantum"
	"github.com/prometheus/prometheus/prompb"
)

func Save(db *store.Store, b *Batch, ts time.Time) error {
	txn := db.DB.NewTransaction(true)
	defer txn.Discard()
	return store.UpdateIndex(db.Index, func(tx *rbf.Tx) error {
		view := quantum.ViewByTimeUnit("", ts, 'D')
		err := batch.Apply(tx, fields.Fragment{ID: fields.MetricsValue, View: view}, b.values)
		if err != nil {
			return err
		}
		err = batch.Apply(tx, fields.Fragment{ID: fields.MetricsHistogram, View: view}, b.histogram)
		if err != nil {
			return err
		}
		err = batch.Apply(tx, fields.Fragment{ID: fields.MetricsTimestamp, View: view}, b.timestamp)
		if err != nil {
			return err
		}
		err = batch.Apply(tx, fields.Fragment{ID: fields.MetricsSeries, View: view}, b.series)
		if err != nil {
			return err
		}
		err = batch.Apply(tx, fields.Fragment{ID: fields.MetricsLabels, View: view}, b.labels)
		if err != nil {
			return err
		}
		err = batch.Apply(tx, fields.Fragment{ID: fields.MetricsExemplars, View: view}, b.exemplars)
		if err != nil {
			return err
		}
		err = batch.Apply(tx, fields.Fragment{ID: fields.MetricsFSTBitmap, View: view}, b.fst)
		if err != nil {
			return err
		}
		err = batch.ApplyFST(txn, tx, blob.Translate(txn),
			fields.Fragment{ID: fields.MetricsFST, View: view},
			fields.Fragment{ID: fields.MetricsFSTBitmap, View: view},
			b.fst)
		if err != nil {
			return err
		}
		return batch.ApplyShards(tx, fields.Fragment{ID: fields.MetricsShards, View: view}, &b.shards)
	})
}

func UpsertLabels(b blob.Func) LabelFunc {
	m := roaring64.New()
	var h bytes.Buffer
	return func(l []prompb.Label) []uint64 {
		m.Clear()
		for i := range l {
			h.Reset()
			h.WriteString(l[i].Name)
			h.WriteByte('=')
			h.WriteString(l[i].Value)
			m.Add(b(constants.MetricsLabels, bytes.Clone(h.Bytes())))
		}
		return m.ToArray()
	}
}
