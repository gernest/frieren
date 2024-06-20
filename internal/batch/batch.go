package batch

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/bits"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/blevesearch/vellum"
	"github.com/dgraph-io/badger/v4"
	v1 "github.com/gernest/frieren/gen/go/fri/v1"
	"github.com/gernest/frieren/internal/blob"
	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/frieren/internal/fields"
	"github.com/gernest/frieren/internal/keys"
	"github.com/gernest/frieren/internal/ro"
	"github.com/gernest/frieren/internal/self"
	"github.com/gernest/frieren/internal/store"
	"github.com/gernest/rbf"
	"github.com/gernest/roaring"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/protobuf/proto"
)

type Mapping map[uint64]*roaring.Bitmap

type Batch struct {
	fields map[constants.ID]Mapping
	exists map[constants.ID]Mapping
	depth  map[uint64]map[uint64]uint64
	shards roaring64.Bitmap
	Rows   int64
}

func NewBatch() *Batch {
	return batchPool.Get().(*Batch)
}

var batchPool = &sync.Pool{New: func() any {
	return &Batch{
		fields: make(map[constants.ID]Mapping),
		exists: make(map[constants.ID]Mapping),
		depth:  make(map[uint64]map[uint64]uint64),
	}
}}

func (b *Batch) Release() {
	b.Reset()
	batchPool.Put(b)
	b.Rows = 0
}

func (b *Batch) Reset() {
	clear(b.fields)
	clear(b.depth)
	b.shards.Clear()
}

type Func func(field constants.ID, mapping map[uint64]*roaring.Bitmap) error

func (b *Batch) Apply(db *store.Store, resource constants.Resource, view string, cb AppendCallback) error {
	return db.DB.Update(func(txn *badger.Txn) error {
		tx, err := db.Index.Begin(true)
		if err != nil {
			return err
		}
		err = cb(txn, tx, b)
		if err != nil {
			tx.Rollback()
			return err
		}
		err = b.Range(func(field constants.ID, mapping map[uint64]*roaring.Bitmap) error {
			err := Apply(tx, fields.New(field, 0, view), mapping)
			if err != nil {
				return err
			}
			switch field {
			case constants.MetricsLabels, constants.TracesLabels, constants.LogsLabels:
				return ApplyFST(txn, tx, blob.Translate(txn, db, view), fields.New(field, 0, view), mapping)
			default:
				return nil
			}
		})
		if err != nil {
			tx.Rollback()
			return err
		}
		err = b.RangeExists(func(field constants.ID, mapping map[uint64]*roaring.Bitmap) error {
			return Apply(tx, fields.New(field, 0, view+"_exists"), mapping)
		})
		if err != nil {
			tx.Rollback()
			return err
		}
		err = tx.Commit()
		if err != nil {
			return err
		}
		return ApplyBitDepth(txn, resource, view, b.GetDepth())
	})
}

func (b *Batch) Range(f Func) error {
	for field, v := range b.fields {
		err := f(field, v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *Batch) RangeExists(f Func) error {
	for field, v := range b.exists {
		err := f(field, v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *Batch) GetDepth() map[uint64]map[uint64]uint64 {
	return b.depth
}

func (b *Batch) GetShards() *roaring64.Bitmap {
	return &b.shards
}

func (b *Batch) Depth(field constants.ID, shard uint64, depth int) {
	m, ok := b.depth[shard]
	if !ok {
		m = make(map[uint64]uint64)
		b.depth[shard] = m
	}
	m[uint64(field)] = max(m[uint64(field)], uint64(depth))
}

func (b *Batch) Shard(shard uint64) {
	b.shards.Add(shard)
}

func (b *Batch) BSI(field constants.ID, shard, id, value uint64) {
	ro.BSI(b.bitmap(field, shard), id, value)
	b.Depth(field, shard, bits.Len64(value))
}

func (b *Batch) Mutex(field constants.ID, shard, id, value uint64) {
	ro.Mutex(b.bitmap(field, shard), id, value)
}

func (b *Batch) Bool(field constants.ID, shard, id uint64, value bool) {
	ro.Bool(b.bitmap(field, shard), id, value)
}

func (b *Batch) Set(field constants.ID, shard, id uint64, value []uint64) {
	ro.Set(b.bitmap(field, shard), b.existence(field, shard), id, value)
}

func (b *Batch) SetBitmap(field constants.ID, shard, id uint64, value *roaring.Bitmap) {
	ro.SetBitmap(b.bitmap(field, shard), b.existence(field, shard), id, value)
}

func (b *Batch) Add(field constants.ID, shard, value uint64) {
	b.bitmap(field, shard).Add(value)
}

func (b *Batch) existence(field constants.ID, u uint64) *roaring.Bitmap {
	return b.bitmapBase(b.exists, field, u)
}

func (b *Batch) bitmap(field constants.ID, u uint64) *roaring.Bitmap {
	return b.bitmapBase(b.fields, field, u)
}

func (b *Batch) bitmapBase(base map[constants.ID]Mapping, field constants.ID, u uint64) *roaring.Bitmap {
	m, ok := base[field]
	if !ok {
		m = make(map[uint64]*roaring.Bitmap)
		base[field] = m
	}
	r, ok := m[u]
	if !ok {
		r = roaring.NewBitmap()
		m[u] = r
	}
	return r
}

func Apply(tx *rbf.Tx, view *fields.Fragment, data map[uint64]*roaring.Bitmap) error {
	for shard, m := range data {
		key := view.WithShard(shard).String()
		_, err := tx.AddRoaring(key, m)
		if err != nil {
			return fmt.Errorf("adding to %s %w", key, err)
		}
	}
	return nil
}

func ApplyFST(txn *badger.Txn, tx *rbf.Tx, tr blob.Tr, field *fields.Fragment, data map[uint64]*roaring.Bitmap) error {
	buf := new(bytes.Buffer)
	b := roaring64.New()
	for shard, _ := range data {
		err := updateFST(buf, tx, txn, tr, field.WithShard(shard), b)
		if err != nil {
			return fmt.Errorf("inserting exists bsi %w", err)
		}
	}
	return nil
}

func updateFST(buf *bytes.Buffer, tx *rbf.Tx, txn *badger.Txn, tr blob.Tr, field *fields.Fragment, bm *roaring64.Bitmap) error {

	fstKey := bytes.Clone(keys.FST(buf, field.ID, field.Shard, field.View))

	// We already save this. It is safe to reuse
	bm.Clear()
	err := field.RowsBitmap(tx, 0, bm)
	if err != nil {
		return err
	}

	keys := make([][]byte, bm.GetCardinality())
	values := bm.ToArray()
	for i := range values {
		keys[i] = tr(constants.MetricsLabels, values[i])
	}
	sort.Sort(&fstValues{keys: keys, values: values})
	bs, err := vellum.New(buf, nil)
	if err != nil {
		return fmt.Errorf("opening fst builder %w", err)
	}
	for i := range keys {
		err = bs.Insert(keys[i], values[i])
		if err != nil {
			return fmt.Errorf("inserting fst key key=%q %w", string(keys[i]), err)
		}
	}
	err = bs.Close()
	if err != nil {
		return fmt.Errorf("closing fst builder %w", err)
	}
	err = txn.Set(fstKey, bytes.Clone(buf.Bytes()))
	if err != nil {
		return fmt.Errorf("saving fst %w", err)
	}
	return nil
}

type fstValues struct {
	values []uint64
	keys   [][]byte
}

func (f *fstValues) Len() int {
	return len(f.keys)
}

func (f *fstValues) Less(i, j int) bool {
	return bytes.Compare(f.keys[i], f.keys[j]) == -1
}

func (f *fstValues) Swap(i, j int) {
	f.keys[i], f.keys[j] = f.keys[j], f.keys[i]
	f.values[i], f.values[j] = f.values[j], f.values[i]
}

func ApplyBitDepth(txn *badger.Txn, resource constants.Resource, view string, depth map[uint64]map[uint64]uint64) error {
	b := &v1.FieldViewInfo{}
	key := keys.FieldView(new(bytes.Buffer), resource, view)
	store.Get(txn, key, func(val []byte) error {
		return proto.Unmarshal(val, b)
	})
	if b.Shards == nil {
		b.Shards = make(map[uint64]*v1.Shard)
		for shard, set := range depth {
			b.Shards[shard] = &v1.Shard{
				BitDepth: set,
			}
		}
	} else {
		for shard, set := range depth {
			s, ok := b.Shards[shard]
			if !ok {
				s = &v1.Shard{
					Id:       shard,
					BitDepth: make(map[uint64]uint64),
				}
			}
			for k, v := range set {
				s.BitDepth[k] = max(s.BitDepth[k], v)
			}
		}
	}
	data, err := proto.Marshal(b)
	if err != nil {
		return err
	}
	return txn.Set(key, data)
}

func FieldViewInfo(txn *badger.Txn, resource constants.Resource, view string) (*v1.FieldViewInfo, error) {
	key := keys.FieldView(new(bytes.Buffer), resource, view)
	it, err := txn.Get(key)
	if err != nil {
		if !errors.Is(err, badger.ErrKeyNotFound) {
			return nil, err
		}
		return &v1.FieldViewInfo{}, nil
	}
	o := &v1.FieldViewInfo{}
	err = it.Value(func(val []byte) error {
		return proto.Unmarshal(val, o)
	})
	if err != nil {
		return nil, err
	}
	return o, nil
}

var (
	batchRowsAdded metric.Int64Counter
	batchDuration  metric.Float64Histogram
	batchFailure   metric.Int64Counter
	batchOnce      sync.Once
)

func initMetrics() {
	batchOnce.Do(func() {
		batchRowsAdded = self.Counter("batch.rows",
			metric.WithDescription("Total number of rows added"),
		)
		batchDuration = self.FloatHistogram("batch.duration",
			metric.WithDescription("Time in seconds of processing the batch"),
			metric.WithUnit("s"),
		)
		batchFailure = self.Counter("batch.failure",
			metric.WithDescription("Total number errored batch process"),
		)
	})
}

type AppendCallback func(txn *badger.Txn, tx *rbf.Tx, bx *Batch) error

func Append(
	ctx context.Context,
	resource constants.Resource,
	store *store.Store,
	view string,
	f AppendCallback,
) error {
	ctx, span := self.Start(ctx, fmt.Sprintf("%s.batch", strings.ToLower(resource.String())))
	defer span.End()
	start := time.Now()
	res := attribute.String("otel.resource", resource.String())
	defer func() {
		batchDuration.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(res))
	}()
	bx := NewBatch()
	defer bx.Release()

	initMetrics()

	err := bx.Apply(store, resource, view, f)
	if err != nil {
		batchFailure.Add(ctx, 1, metric.WithAttributes(res))
		return err
	}
	batchRowsAdded.Add(ctx, bx.Rows, metric.WithAttributes(res))
	return nil
}
