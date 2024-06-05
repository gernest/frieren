package ernestdb

import (
	"bytes"
	"fmt"
	"runtime"
	"slices"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/blevesearch/vellum"
	"github.com/cespare/xxhash/v2"
	"github.com/dgraph-io/ristretto"
	"github.com/gernest/ernestdb/keys"
	"github.com/prometheus/prometheus/prompb"
)

type Store interface {
	Set(key, value []byte) error
	Get(key []byte, value func(val []byte) error) error
}

func Save(db Store, b *Batch) error {
	var buf bytes.Buffer
	tmpBSI := roaring64.NewDefaultBSI()
	var existsKey keys.Exists
	for shard, bsi := range b.exists {
		existsKey.ShardID = shard
		err := UpsertBSI(db, &buf, tmpBSI, bsi, existsKey.Key())
		if err != nil {
			return fmt.Errorf("inserting exists bsi %w", err)
		}
	}
	var valuesKey keys.Value
	for shard, series := range b.values {
		valuesKey.ShardID = shard
		for seriesID, bsi := range series {
			valuesKey.SeriesID = seriesID
			err := UpsertBSI(db, &buf, tmpBSI, bsi, valuesKey.Key())
			if err != nil {
				return fmt.Errorf("inserting exists bsi %w", err)
			}
		}
	}

	var tsKey keys.Timestamp
	for shard, series := range b.timestamp {
		tsKey.ShardID = shard
		for seriesID, bsi := range series {
			tsKey.SeriesID = seriesID
			err := UpsertBSI(db, &buf, tmpBSI, bsi, tsKey.Key())
			if err != nil {
				return fmt.Errorf("inserting exists bsi %w", err)
			}
		}
	}
	var seriesKey keys.Series
	tmpBitmap := roaring64.New()
	for shard, series := range b.series {
		seriesKey.ShardID = shard
		for seriesID, bsi := range series {
			tsKey.SeriesID = seriesID
			err := UpsertBitmap(db, &buf, tmpBitmap, bsi, seriesKey.Key())
			if err != nil {
				return fmt.Errorf("inserting exists bsi %w", err)
			}
		}
	}
	var labelsKey keys.Labels

	for shard, series := range b.labels {
		labelsKey.ShardID = shard
		for labelID, bsi := range series {
			labelsKey.LabelID = labelID
			err := UpsertBitmap(db, &buf, tmpBitmap, bsi, labelsKey.Key())
			if err != nil {
				return fmt.Errorf("inserting exists bsi %w", err)
			}
		}
	}
	var fstKey keys.FSTBitmap
	for shard, bsi := range b.fst {
		fstKey.ShardID = shard
		err := UpsertFST(db, &buf, tmpBitmap, bsi, shard, fstKey.Key())
		if err != nil {
			return fmt.Errorf("inserting exists bsi %w", err)
		}
	}
	return nil
}

func UpsertBSI(db Store, buf *bytes.Buffer, tmp, b *roaring64.BSI, key []byte) error {
	buf.Reset()
	var update bool
	err := db.Get(key, func(val []byte) error {
		update = true
		_, err := tmp.ReadFrom(bytes.NewReader(val))
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
	tmp.ParOr(runtime.NumCPU(), b)
	tmp.RunOptimize()
	_, err = tmp.WriteTo(buf)
	if err != nil {
		return err
	}
	return db.Set(key, bytes.Clone(buf.Bytes()))
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
	for i := range o {
		err = bs.Insert(o[i], 0)
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
