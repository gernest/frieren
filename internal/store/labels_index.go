package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"path/filepath"
	"slices"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/gernest/rbf"
	"github.com/gernest/rbf/dsl/bsi"
	"github.com/gernest/rbf/dsl/mutex"
	rroaring "github.com/gernest/roaring"
	"github.com/gernest/roaring/shardwidth"
	"go.etcd.io/bbolt"
)

var (
	sep          = []byte("=")
	idsBucket    = []byte("\x00i")
	blobBucket   = []byte("\x00b")
	labelsBucket = []byte("\x00l")
	metaBucket   = []byte("\x00m")
	root         = []byte("\x00")
)

var emptyKey = []byte{
	0x00, 0x00, 0x00,
	0x4d, 0x54, 0x4d, 0x54, // MTMT
	0x00,
	0xc2, 0xa0, // NO-BREAK SPACE
	0x00,
}

type Batch struct {
	blobs   []blob
	values  map[uint32]*Value
	buffer  roaring.Bitmap
	blobSet roaring.Bitmap

	ids        []uint64
	lbls       [][]uint32
	series     []uint32
	histograms []uint32
	exemplars  [][]uint32
	ts         []int64
	vals       []uint64
}

var batchPool = &sync.Pool{New: func() any {
	return &Batch{values: make(map[uint32]*Value)}
}}

func NewBatch() *Batch {
	return batchPool.Get().(*Batch)
}

func (b *Batch) Release() {
	b.Reset()
	batchPool.Put(b)
}

func (b *Batch) Reset() {
	b.blobs = b.blobs[:0]
	clear(b.values)
	b.buffer.Clear()
	b.blobSet.Clear()

	b.ids = b.ids[:0]
	b.lbls = b.lbls[:0]
	b.series = b.series[:0]
	b.histograms = b.histograms[:0]
	b.exemplars = b.exemplars[:0]
	b.histograms = b.histograms[:0]
	b.ts = b.ts[:0]
	b.vals = b.vals[:0]
}

func (b *Batch) Resize(n int) {
	b.ids = slices.Grow(b.ids[:0], n)
	b.lbls = slices.Grow(b.lbls[:0], n)
	b.series = slices.Grow(b.series[:0], n)
	b.histograms = slices.Grow(b.histograms[:0], n)
	b.exemplars = slices.Grow(b.exemplars[:0], n)
	b.histograms = slices.Grow(b.histograms[:0], n)
	b.ts = slices.Grow(b.ts[:0], n)
	b.vals = slices.Grow(b.vals[:0], n)
}

type blob struct {
	data []byte
	hash uint32
}

func (db *DB) Get(ids []uint32, f func(pos int, value []byte) error) error {
	return db.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(blobBucket)
		var buf [4]byte
		for i := range ids {
			binary.BigEndian.PutUint32(buf[:], ids[i])
			err := f(i, b.Get(buf[:]))
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (b *Batch) Blob(data []byte) uint32 {
	if len(data) == 0 {
		data = emptyKey
	}
	hash := Hash32(func(h hash.Hash32) {
		h.Write(data)
	})
	if b.blobSet.Contains(hash) {
		return hash
	}
	b.blobSet.Add(hash)
	b.blobs = append(b.blobs, blob{
		data: data,
		hash: hash,
	})
	return hash
}

type Labels interface {
	Write(key, value []byte)
}

type LabelsFunc func(key, value []byte)

func (f LabelsFunc) Write(key, value []byte) {
	f(key, value)
}

func (b *Batch) Write(f func(w Labels)) (labels []uint32, series uint32) {
	b.buffer.Clear()
	values := map[*Value]struct{}{}
	var buf bytes.Buffer
	b.buffer.Clear()
	f(LabelsFunc(func(key, value []byte) {
		h := Hash32(func(h hash.Hash32) {
			h.Write(key)
			h.Write(sep)
			h.Write(value)
		})
		if !b.blobSet.Contains(h) {
			// add blob
			b.blobSet.Add(h)
			buf.Reset()
			buf.Write(key)
			buf.Write(sep)
			buf.Write(value)
			b.blobs = append(b.blobs, blob{
				data: bytes.Clone(buf.Bytes()),
				hash: h,
			})
		}
		b.buffer.Add(h)
		values[b.add(key, value, h)] = struct{}{}
	}))
	series = Hash32(func(h hash.Hash32) {
		b.buffer.RunOptimize()
		b.buffer.WriteTo(h)
	})
	labels = b.buffer.ToArray()

	// Update series
	for k := range values {
		k.series.Add(series)
	}
	return
}

func (b *Batch) apply(f func(value *Value) error) error {
	for _, k := range b.values {
		err := f(k)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *Batch) add(key, value []byte, hash uint32) *Value {
	if k, ok := b.values[hash]; ok {
		return k
	}
	v := &Value{key: key, value: value, hash: hash}
	b.values[hash] = v
	return v
}

type Value struct {
	key    []byte
	value  []byte
	hash   uint32
	series roaring.Bitmap
}

type DB struct {
	db  *bbolt.DB
	idx *rbf.DB

	blobs roaring.Bitmap
	meta  roaring.Bitmap
}

func New(path string) (*DB, error) {

	idx := rbf.NewDB(filepath.Join(path, "rbf"), nil)
	err := idx.Open()
	if err != nil {
		return nil, err
	}
	db, err := bbolt.Open(filepath.Join(path, "OPS"), 0600, nil)
	if err != nil {
		idx.Close()
		return nil, err
	}
	return &DB{db: db, idx: idx}, nil
}

func (db *DB) Close() error {
	return errors.Join(db.db.Close(), db.idx.Close())
}

func (db *DB) apply(b *Batch, f ...func(tx *bbolt.Tx) error) error {

	return db.db.Update(func(tx *bbolt.Tx) error {
		if len(b.ids) > 0 {
			ids, err := bucket(tx, idsBucket)
			if err != nil {
				return err
			}
			for i := range b.ids {
				b.ids[i], _ = ids.NextSequence()
			}
			txn, err := db.idx.Begin(true)
			if err != nil {
				return err
			}
			err = each(b.ids, func(shard uint64, start, end int) error {
				{
					// labels
					a := rroaring.NewBitmap()
					for i := start; i < end; i++ {
						for j := range b.lbls[i] {
							mutex.Add(a, b.ids[i], uint64(b.lbls[i][j]))
						}
					}
					_, err := txn.AddRoaring(fmt.Sprintf("labels:%d", shard), a)
					if err != nil {
						return err
					}
				}
				{
					// save series
					a := rroaring.NewBitmap()
					for i := start; i < end; i++ {
						mutex.Add(a, b.ids[i], uint64(b.series[i]))
					}
					_, err := txn.AddRoaring(fmt.Sprintf("series:%d", shard), a)
					if err != nil {
						return err
					}
				}
				{
					// histogram
					a := rroaring.NewBitmap()
					for i := start; i < end; i++ {
						if b.histograms[i] == 0 {
							continue
						}
						mutex.Add(a, b.ids[i], uint64(b.histograms[i]))
					}
					_, err := txn.AddRoaring(fmt.Sprintf("histogram:%d", shard), a)
					if err != nil {
						return err
					}
				}
				{
					// exemplars
					a := rroaring.NewBitmap()
					for i := start; i < end; i++ {
						for j := range b.exemplars[i] {
							mutex.Add(a, b.ids[i], uint64(b.exemplars[i][j]))
						}
					}
					_, err := txn.AddRoaring(fmt.Sprintf("exemplars:%d", shard), a)
					if err != nil {
						return err
					}
				}
				{
					// timestamp
					a := rroaring.NewBitmap()
					for i := start; i < end; i++ {
						bsi.Add(a, b.ids[i], b.ts[i])
					}
					_, err := txn.AddRoaring(fmt.Sprintf("timestamp:%d", shard), a)
					if err != nil {
						return err
					}
				}
				{
					// values
					a := rroaring.NewBitmap()
					for i := start; i < end; i++ {
						bsi.Add(a, b.ids[i], int64(b.vals[i]))
					}
					_, err := txn.AddRoaring(fmt.Sprintf("values:%d", shard), a)
					if err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				txn.Rollback()
				return err
			}
			err = txn.Commit()
			if err != nil {
				return err
			}
		}

		blob, err := bucket(tx, blobBucket)
		if err != nil {
			return err
		}
		labels, err := bucket(tx, labelsBucket)
		if err != nil {
			return err
		}

		// save blobs
		var buf [4]byte
		for i := range b.blobs {
			if db.blobs.Contains(b.blobs[i].hash) {
				continue
			}
			binary.BigEndian.PutUint32(buf[:], b.blobs[i].hash)
			err = blob.Put(buf[:], b.blobs[i].data)
			if err != nil {
				return err
			}
		}

		// Take advantage of write tx lock and safely update seen blobs
		db.blobs.Or(&b.blobSet)

		for i := range f {
			err = f[i](tx)
			if err != nil {
				return err
			}
		}

		// save labels
		return b.apply(func(v *Value) error {
			kx, _, err := bbucket(labels, v.key)
			if err != nil {
				return err
			}
			vx, exists, err := bbucket(kx, v.value)
			if err != nil {
				return err
			}
			if exists {
				// we have seen this label before. Load existing data
				o := roaring.New()
				o.UnmarshalBinary(vx.Get(root))
				v.series.Or(o)
			}
			data, err := v.series.MarshalBinary()
			if err != nil {
				return err
			}
			return vx.Put(root, data)
		})
	})
}

func bucket(tx *bbolt.Tx, key []byte) (*bbolt.Bucket, error) {
	tx.Cursor()
	if b := tx.Bucket(key); b != nil {
		return b, nil
	}
	return tx.CreateBucket(key)
}

func bbucket(tx *bbolt.Bucket, key []byte) (*bbolt.Bucket, bool, error) {
	if b := tx.Bucket(key); b != nil {
		return b, true, nil
	}
	o, err := tx.CreateBucket(key)
	return o, false, err
}

// iterate on shards
func each(ids []uint64, f func(shard uint64, start, end int) error) error {
	for start, end := 0,
		shardwidth.FindNextShard(0, ids); start < len(ids) && end <= len(ids); start, end = end,
		shardwidth.FindNextShard(end, ids) {
		shard := ids[start] / shardwidth.ShardWidth
		err := f(shard, start, end)
		if err != nil {
			return err
		}
	}
	return nil
}
