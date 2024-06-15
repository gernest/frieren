package batch

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/bits"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/blevesearch/vellum"
	"github.com/cespare/xxhash/v2"
	"github.com/dgraph-io/badger/v4"
	v1 "github.com/gernest/frieren/gen/go/fri/v1"
	"github.com/gernest/frieren/internal/blob"
	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/frieren/internal/fields"
	"github.com/gernest/frieren/internal/keys"
	"github.com/gernest/frieren/internal/ro"
	"github.com/gernest/frieren/internal/self"
	"github.com/gernest/frieren/internal/store"
	"github.com/gernest/frieren/internal/util"
	"github.com/gernest/rbf"
	"github.com/gernest/rbf/quantum"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/protobuf/proto"
)

type Mapping map[uint64]*roaring64.Bitmap

type Batch struct {
	fields map[constants.ID]Mapping
	depth  map[uint64]map[uint64]uint64
	shards roaring64.Bitmap
	Rows   int64
}

func NewBatch() *Batch {
	return batchPool.Get().(*Batch)
}

var batchPool = &sync.Pool{New: func() any {
	return &Batch{fields: make(map[constants.ID]Mapping),
		depth: make(map[uint64]map[uint64]uint64)}
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

type Func func(field constants.ID, mapping map[uint64]*roaring64.Bitmap) error

func (b *Batch) Apply(db *store.Store, resource constants.Resource, view string, cb ...func(txn *badger.Txn) error) error {
	return db.DB.Update(func(txn *badger.Txn) error {
		tx, err := db.Index.Begin(true)
		if err != nil {
			return err
		}
		err = b.Range(func(field constants.ID, mapping map[uint64]*roaring64.Bitmap) error {
			switch field {
			case constants.MetricsFST, constants.TracesFST, constants.LogsFST:
				return ApplyFST(txn, tx, blob.Translate(txn), view, field, mapping)
			default:
				return Apply(tx, fields.New(field, 0, view), mapping)
			}
		})
		if err != nil {
			tx.Rollback()
			return err
		}
		err = tx.Commit()
		if err != nil {
			return err
		}
		err = ApplyBitDepth(txn, resource, view, b.GetDepth())
		if err != nil {
			return err
		}
		if len(cb) > 0 {
			return cb[0](txn)
		}
		return nil
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

func (b *Batch) BSISet(field constants.ID, shard, id uint64, value []uint64) {
	ro.BSISet(b.bitmap(field, shard), id, value)
}

func (b *Batch) Add(field constants.ID, shard, value uint64) {
	b.bitmap(field, shard).Add(value)
}

func (b *Batch) AddMany(field constants.ID, shard uint64, value []uint64) {
	b.bitmap(field, shard).AddMany(value)
}

func (b *Batch) bitmap(field constants.ID, u uint64) *roaring64.Bitmap {
	m, ok := b.fields[field]
	if !ok {
		m = make(map[uint64]*roaring64.Bitmap)
		b.fields[field] = m
	}
	r, ok := m[u]
	if !ok {
		r = roaring64.New()
		m[u] = r
	}
	return r
}

func Apply(tx *rbf.Tx, view *fields.Fragment, data map[uint64]*roaring64.Bitmap) error {
	for shard, m := range data {
		key := view.WithShard(shard).String()
		_, err := tx.Add(key, m.ToArray()...)
		if err != nil {
			return fmt.Errorf("adding to %s %w", key, err)
		}
	}
	return nil
}

func ApplyFST(txn *badger.Txn, tx *rbf.Tx, tr blob.Tr, view string, id constants.ID, data map[uint64]*roaring64.Bitmap) error {
	buf := new(bytes.Buffer)
	for shard, bm := range data {
		err := updateFST(buf, txn, tr, shard, view, id, bm)
		if err != nil {
			return fmt.Errorf("inserting exists bsi %w", err)
		}
	}
	return nil
}

func updateFST(buf *bytes.Buffer, txn *badger.Txn, tr blob.Tr, shard uint64, view string, id constants.ID, bm *roaring64.Bitmap) error {

	bitmapKey := bytes.Clone(keys.FSTBitmap(buf, id, shard, view))
	fstKey := bytes.Clone(keys.FST(buf, id, shard, view))

	b := roaring64.New()
	err := store.Get(txn, bitmapKey, b.UnmarshalBinary)
	if err != nil {
		if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
	}
	b.Or(bm)

	o := make([][]byte, 0, b.GetCardinality())
	it := b.Iterator()
	for it.HasNext() {
		id := it.Next()
		err := tr(constants.MetricsLabels, id, func(val []byte) error {
			o = append(o, bytes.Clone(val))
			return nil
		})
		if err != nil {
			util.Exit("translating label", "id", id, "err", err)
		}
	}
	slices.SortFunc(o, bytes.Compare)
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
	b.RunOptimize()
	fstData := bytes.Clone(buf.Bytes())
	buf.Reset()
	b.WriteTo(buf)
	err = txn.Set(fstKey, fstData)
	if err != nil {
		return fmt.Errorf("saving fst %w", err)
	}
	err = txn.Set(bitmapKey, buf.Bytes())
	if err != nil {
		return fmt.Errorf("saving fst bitmap %w", err)
	}
	return nil
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

func Append(
	ctx context.Context,
	resource constants.Resource,
	store *store.Store,
	ts time.Time,
	f func(bx *Batch) error,
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

	err := f(bx)
	if err != nil {
		return err
	}
	initMetrics()
	view := quantum.ViewByTimeUnit("", ts, 'D')

	err = bx.Apply(store, resource, view)
	if err != nil {
		batchFailure.Add(ctx, 1, metric.WithAttributes(res))
		return err
	}
	batchRowsAdded.Add(ctx, bx.Rows, metric.WithAttributes(res))
	return nil
}
