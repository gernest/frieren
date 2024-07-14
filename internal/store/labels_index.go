package store

import (
	"bytes"
	"encoding/binary"
	"hash"

	"github.com/RoaringBitmap/roaring"
	"go.etcd.io/bbolt"
)

var (
	sep          = []byte("=")
	blobBucket   = []byte("\x00b")
	labelsBucket = []byte("\x00l")
	root         = []byte("\x00")
)

type Batch struct {
	blobs   []blob
	values  map[uint32]*Value
	labels  roaring.Bitmap
	blobSet roaring.Bitmap
}

func NewBatch() *Batch {
	return &Batch{values: make(map[uint32]*Value)}
}

func (b *Batch) Reset() {
	b.blobs = b.blobs[:0]
	clear(b.values)
	b.labels.Clear()
	b.blobSet.Clear()
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
	b.labels.Clear()
	values := map[*Value]struct{}{}
	var buf bytes.Buffer
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
		b.labels.Add(h)
		values[b.add(key, value, h)] = struct{}{}
	}))
	series = Hash32(func(h hash.Hash32) {
		b.labels.RunOptimize()
		b.labels.WriteTo(h)
	})
	labels = b.labels.ToArray()

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
	db    *bbolt.DB
	blobs roaring.Bitmap
}

func New(path string) (*DB, error) {
	db, err := bbolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}
	return &DB{db: db}, nil
}

func (db *DB) Close() error {
	return db.db.Close()
}

func (db *DB) Apply(b *Batch) error {
	return db.db.Update(func(tx *bbolt.Tx) error {
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
