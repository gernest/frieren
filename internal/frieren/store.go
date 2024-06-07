package frieren

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/blevesearch/vellum"
	"github.com/cespare/xxhash/v2"
	"github.com/dgraph-io/badger/v4"
	"github.com/gernest/frieren/internal/keys"
	"github.com/gernest/rbf"
	"github.com/gernest/rbf/quantum"
	"github.com/prometheus/prometheus/prompb"
)

type Store struct {
	DB    *badger.DB
	Index *rbf.DB
	Seq   *Seq
}

func NewStore(path string) (*Store, error) {
	dbPath := filepath.Join(path, "db")

	db, err := badger.Open(badger.DefaultOptions(dbPath).
		WithLogger(nil))
	if err != nil {
		return nil, err
	}
	idxPath := filepath.Join(path, "index")
	idx := rbf.NewDB(idxPath, nil)
	err = idx.Open()
	if err != nil {
		return nil, err
	}
	seq, err := NewSequence(db)
	if err != nil {
		return nil, err
	}
	return &Store{DB: db, Index: idx, Seq: seq}, nil
}

func (s *Store) Close() error {
	return errors.Join(
		s.Seq.Release(), s.Index.Close(), s.DB.Close(),
	)
}

type Value interface {
	Value(f func(val []byte) error) error
}

func Save(db *badger.DB, idx *rbf.DB, b *Batch, ts time.Time) error {
	err := UpdateIndex(idx, func(tx *rbf.Tx) error {
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
		err = apply(tx, "metrics.exists", view, b.exists)
		if err != nil {
			return err
		}
		return applyShards(tx, "metrics.shards", view, &b.shards)
	})
	if err != nil {
		return err
	}
	return db.Update(func(txn *badger.Txn) error {
		var fstKey keys.FSTBitmap
		var buf bytes.Buffer
		tmpBitmap := roaring64.New()
		for shard, bsi := range b.fst {
			fstKey.ShardID = shard
			err := UpsertFST(txn, &buf, tmpBitmap, bsi, shard, fstKey.Key())
			if err != nil {
				return fmt.Errorf("inserting exists bsi %w", err)
			}
		}
		return nil
	})
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

func applyShards(tx *rbf.Tx, field, view string, m *roaring64.Bitmap) error {
	key := viewFor(field, view, 0)
	_, err := tx.Add(key, m.ToArray()...)
	if err != nil {
		return fmt.Errorf("adding to %s %w", key, err)
	}
	return nil
}

func UpsertBitmap(txn *badger.Txn, buf *bytes.Buffer, tmp, b *roaring64.Bitmap, key []byte) error {
	buf.Reset()
	if Has(txn, key) {
		tmp.Clear()
		err := Get(txn, key, tmp.UnmarshalBinary)
		if err != nil {
			return err
		}
		b.Or(tmp)
	}
	b.RunOptimize()
	_, err := b.WriteTo(buf)
	if err != nil {
		return err
	}
	return txn.Set(key, bytes.Clone(buf.Bytes()))
}

func UpsertFST(txn *badger.Txn, buf *bytes.Buffer, tmp, b *roaring64.Bitmap, shard uint64, key []byte) error {
	if Has(txn, key) {
		tmp.Clear()
		err := Get(txn, key, tmp.UnmarshalBinary)
		if err != nil {
			return err
		}
		b.Or(tmp)
	}

	// Build FST
	o := make([][]byte, 0, b.GetCardinality())

	slice := (&keys.Blob{}).Slice()
	kb := make([]byte, 0, len(slice)*7)
	it := b.Iterator()
	if it.HasNext() {
		slice[1] = it.Next()
		value := value(txn, keys.Encode(kb, slice))
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

func value(txn *badger.Txn, key []byte) (o []byte) {
	Get(txn, key, func(val []byte) error {
		o = bytes.Clone(val)
		return nil
	})
	return
}

func UpsertBlob(txn *badger.Txn) BlobFunc {
	h := xxhash.New()
	var key keys.Blob
	return func(b []byte) uint64 {
		h.Reset()
		h.Write(b)
		key.BlobID = h.Sum64()
		k := key.Key()
		if Has(txn, k) {
			return key.BlobID
		}
		err := txn.Set(key.Key(), b)
		if err != nil {
			panic(fmt.Sprintf("failed saving blob to storage %v", err))
		}
		return key.BlobID
	}
}

func Translate(txn *badger.Txn) Tr {
	slice := (&keys.Blob{}).Slice()
	buf := make([]byte, 0, len(slice)*8)
	return func(u uint64, f func([]byte) error) error {
		slice[len(slice)-1] = u
		it, err := txn.Get(keys.Encode(buf, slice))
		if err != nil {
			return err
		}
		return it.Value(f)
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
