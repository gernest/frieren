package metrics

import (
	"bytes"
	"fmt"
	"slices"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/blevesearch/vellum"
	"github.com/cespare/xxhash/v2"
	"github.com/dgraph-io/badger/v4"
	"github.com/gernest/frieren/internal/blob"
	"github.com/gernest/frieren/internal/fields"
	"github.com/gernest/frieren/internal/keys"
	"github.com/gernest/frieren/internal/store"
	"github.com/gernest/frieren/util"
	"github.com/gernest/rbf"
	"github.com/gernest/rbf/quantum"
	"github.com/prometheus/prometheus/prompb"
)

func Save(db *store.Store, b *Batch, ts time.Time) error {
	txn := db.DB.NewTransaction(true)
	defer txn.Discard()
	return store.UpdateIndex(db.Index, func(tx *rbf.Tx) error {
		view := quantum.ViewByTimeUnit("", ts, 'D')
		err := apply(tx, fields.Fragment{ID: fields.MetricsValue, View: view}, b.values)
		if err != nil {
			return err
		}
		err = apply(tx, fields.Fragment{ID: fields.MetricsKind, View: view}, b.kind)
		if err != nil {
			return err
		}
		err = apply(tx, fields.Fragment{ID: fields.MetricsTimestamp, View: view}, b.timestamp)
		if err != nil {
			return err
		}
		err = apply(tx, fields.Fragment{ID: fields.MetricsSeries, View: view}, b.series)
		if err != nil {
			return err
		}
		err = apply(tx, fields.Fragment{ID: fields.MetricsLabels, View: view}, b.labels)
		if err != nil {
			return err
		}
		err = apply(tx, fields.Fragment{ID: fields.MetricsExemplars, View: view}, b.exemplars)
		if err != nil {
			return err
		}
		err = apply(tx, fields.Fragment{ID: fields.MetricsExists, View: view}, b.exists)
		if err != nil {
			return err
		}
		err = apply(tx, fields.Fragment{ID: fields.MetricsFSTBitmap, View: view}, b.fst)
		if err != nil {
			return err
		}
		err = applyFST(txn, blob.Translate(txn), fields.Fragment{ID: fields.MetricsFST, View: view}, b.fst)
		if err != nil {
			return err
		}
		return applyShards(tx, fields.Fragment{ID: fields.MetricsShards, View: view}, &b.shards)
	})
}

func applyFST(txn *badger.Txn, tr blob.Tr, view fields.Fragment, data map[uint64]*roaring64.Bitmap) error {
	var buf bytes.Buffer
	tmpBitmap := roaring64.New()
	for shard, m := range data {
		key := view.WithShard(shard).String()
		err := UpsertFST(txn, tr, &buf, tmpBitmap, m, shard, []byte(key))
		if err != nil {
			return fmt.Errorf("inserting exists bsi %w", err)
		}
	}
	return txn.Commit()
}

func apply(tx *rbf.Tx, view fields.Fragment, data map[uint64]*roaring64.Bitmap) error {
	for shard, m := range data {
		key := view.WithShard(shard).String()
		_, err := tx.Add(key, m.ToArray()...)
		if err != nil {
			return fmt.Errorf("adding to %s %w", key, err)
		}
	}
	return nil
}

func applyShards(tx *rbf.Tx, view fields.Fragment, m *roaring64.Bitmap) error {
	key := view.WithShard(0).String()
	_, err := tx.Add(key, m.ToArray()...)
	if err != nil {
		return fmt.Errorf("adding to %s %w", key, err)
	}
	return nil
}

func UpsertFST(txn *badger.Txn, tr blob.Tr, buf *bytes.Buffer, tmp, b *roaring64.Bitmap, shard uint64, key []byte) error {
	if store.Has(txn, key) {
		tmp.Clear()
		err := store.Get(txn, key, tmp.UnmarshalBinary)
		if err != nil {
			return err
		}
		b.Or(tmp)
	}

	// Build FST
	o := make([][]byte, 0, b.GetCardinality())
	it := b.Iterator()
	if it.HasNext() {
		err := tr(it.Next(), func(val []byte) error {
			o = append(o, bytes.Clone(val))
			return nil
		})
		if err != nil {
			util.Exit("translating label", "err", err)
		}
	}
	slices.SortFunc(o, bytes.Compare)

	// store the bitmap
	tmp.RunOptimize()
	buf.Reset()
	tmp.WriteTo(buf)

	err := txn.Set(key, bytes.Clone(buf.Bytes()))
	if err != nil {
		return err
	}

	// store fst
	buf.Reset()
	bs, err := vellum.New(buf, nil)
	if err != nil {
		return fmt.Errorf("opening fst builder %w", err)
	}
	var h xxhash.Digest
	for i := range o {
		h.Reset()
		h.Write(o[i])
		err = bs.Insert(o[i], h.Sum64())
		if err != nil {
			return fmt.Errorf("inserting fst key key=%q %w", string(o[i]), err)
		}
	}
	err = bs.Close()
	if err != nil {
		return fmt.Errorf("closing fst builder %w", err)
	}
	return txn.Set((&keys.FST{ShardID: shard}).Key(), bytes.Clone(buf.Bytes()))
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
			m.Add(b(bytes.Clone(h.Bytes())))
		}
		return m.ToArray()
	}
}
