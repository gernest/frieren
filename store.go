package ernestdb

import (
	"bytes"
	"fmt"
	"slices"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/blevesearch/vellum"
	"github.com/cespare/xxhash/v2"
	"github.com/dgraph-io/ristretto"
	"github.com/gernest/ernestdb/keys"
	"github.com/gernest/rbf"
	"github.com/gernest/rbf/quantum"
	"github.com/prometheus/prometheus/prompb"
)

type Value interface {
	Value(f func(val []byte) error) error
}

type Store interface {
	NextID() uint64
	Has(key []byte) bool
	Set(key, value []byte) error
	Get(key []byte, value func(val []byte) error) error
	Prefix(prefix []byte, f func(key []byte, value Value) error) error
	ViewIndex(f func(tx *rbf.Tx) error) error
	UpdateIndex(f func(tx *rbf.Tx) error) error
}

func Save(db Store, b *Batch, ts time.Time) error {
	err := db.UpdateIndex(func(tx *rbf.Tx) error {
		view := quantum.ViewByTimeUnit("", ts, 'D')
		err := apply(tx, "metrics.values", view, b.values)
		if err != nil {
			return err
		}
		err = apply(tx, "metrics.kind", view, b.kind)
		if err != nil {
			return err
		}
		err = apply(tx, "metrics.timestamp", view, b.timestamp)
		if err != nil {
			return err
		}
		err = apply(tx, "metrics.series", view, b.series)
		if err != nil {
			return err
		}
		err = apply(tx, "metrics.labels", view, b.labels)
		if err != nil {
			return err
		}
		err = apply(tx, "metrics.exemplars", view, b.exemplars)
		if err != nil {
			return err
		}
		return apply(tx, "metrics.exists", view, b.exists)
	})
	if err != nil {
		return err
	}

	var fstKey keys.FSTBitmap
	var buf bytes.Buffer
	tmpBitmap := roaring64.New()
	for shard, bsi := range b.fst {
		fstKey.ShardID = shard
		err := UpsertFST(db, &buf, tmpBitmap, bsi, shard, fstKey.Key())
		if err != nil {
			return fmt.Errorf("inserting exists bsi %w", err)
		}
	}
	return UpsertBitmap(db, &buf, tmpBitmap, &b.shards, keys.Shards{}.Key())
}

func apply(tx *rbf.Tx, field, view string, data map[uint64]*roaring64.Bitmap) error {
	for shard, m := range data {
		key := viewFor(field, view, shard)
		_, err := tx.Add(key, m.ToArray()...)
		if err != nil {
			return fmt.Errorf("adding to %s %w", key, err)
		}
	}
	return nil
}

func UpsertBitmap(db Store, buf *bytes.Buffer, tmp, b *roaring64.Bitmap, key []byte) error {
	buf.Reset()
	var update bool
	err := db.Get(key, func(val []byte) error {
		update = true
		_, err := tmp.FromUnsafeBytes(val)
		return err
	})
	if err != nil {
		return err
	}
	if !update {
		b.RunOptimize()
		_, err = b.WriteTo(buf)
		if err != nil {
			return err
		}
		return db.Set(key, bytes.Clone(buf.Bytes()))
	}
	tmp.Or(b)
	tmp.RunOptimize()
	_, err = tmp.WriteTo(buf)
	if err != nil {
		return err
	}
	return db.Set(key, bytes.Clone(buf.Bytes()))
}

func UpsertFST(db Store, buf *bytes.Buffer, tmp, b *roaring64.Bitmap, shard uint64, key []byte) error {
	tmp.Clear()
	db.Get(key, tmp.UnmarshalBinary)
	tmp.Or(b)

	// Build FST
	o := make([][]byte, 0, tmp.GetCardinality())

	slice := (&keys.Blob{}).Slice()
	kb := make([]byte, 0, len(slice)*7)
	it := tmp.Iterator()
	if it.HasNext() {
		slice[1] = it.Next()
		value := value(db, keys.Encode(kb, slice))
		if len(value) == 0 {
			exit("cannot find translated label")
		}
		o = append(o, value)
	}
	slices.SortFunc(o, bytes.Compare)

	// store the bitmap
	tmp.RunOptimize()
	buf.Reset()
	tmp.WriteTo(buf)

	err := db.Set(key, bytes.Clone(buf.Bytes()))
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
	return db.Set((&keys.FST{ShardID: shard}).Key(), bytes.Clone(buf.Bytes()))
}

func value(db Store, key []byte) (o []byte) {
	db.Get(key, func(val []byte) error {
		o = bytes.Clone(val)
		return nil
	})
	return
}

func UpsertBlob(db Store, cache *ristretto.Cache) BlobFunc {
	h := xxhash.New()
	var key keys.Blob
	return func(b []byte) uint64 {
		h.Reset()
		h.Write(b)
		key.BlobID = h.Sum64()
		if _, ok := cache.Get(key.BlobID); ok {
			return key.BlobID
		}
		cache.Set(key.BlobID, 0, 1)

		k := key.Key()
		var set bool
		db.Get(k, func(val []byte) error {
			set = true
			return nil
		})
		if set {
			return key.BlobID
		}
		err := db.Set(key.Key(), b)
		if err != nil {
			panic(fmt.Sprintf("failed saving blob to storage %v", err))
		}
		return key.BlobID
	}
}

func UpsertLabels(b BlobFunc) LabelFunc {
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
