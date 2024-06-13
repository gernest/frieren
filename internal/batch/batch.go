package batch

import (
	"bytes"
	"errors"
	"fmt"
	"slices"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/blevesearch/vellum"
	"github.com/cespare/xxhash/v2"
	"github.com/dgraph-io/badger/v4"
	v1 "github.com/gernest/frieren/gen/go/fri/v1"
	"github.com/gernest/frieren/internal/blob"
	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/frieren/internal/fields"
	"github.com/gernest/frieren/internal/keys"
	"github.com/gernest/frieren/internal/store"
	"github.com/gernest/frieren/internal/util"
	"github.com/gernest/rbf"
	"google.golang.org/protobuf/proto"
)

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
	for shard, bm := range data {
		err := updateFST(txn, tr, shard, view, id, bm)
		if err != nil {
			return fmt.Errorf("inserting exists bsi %w", err)
		}
	}
	return txn.Commit()
}

func updateFST(txn *badger.Txn, tr blob.Tr, shard uint64, view string, id constants.ID, bm *roaring64.Bitmap) error {
	bitmapKey := (&keys.FSTBitmap{ShardID: shard, FieldID: uint64(id)}).Key()
	bitmapKey = append(bitmapKey, []byte(view)...)

	fstKey := (&keys.FST{ShardID: shard, FieldID: uint64(id)}).Key()
	fstKey = append(fstKey, []byte(view)...)

	b := roaring64.New()
	err := store.Get(txn, bitmapKey, b.UnmarshalBinary)
	if err != nil {
		return err
	}
	b.Or(bm)

	o := make([][]byte, 0, b.GetCardinality())
	it := b.Iterator()
	for it.HasNext() {
		err := tr(constants.MetricsFST, it.Next(), func(val []byte) error {
			o = append(o, bytes.Clone(val))
			return nil
		})
		if err != nil {
			util.Exit("translating label", "err", err)
		}
	}
	slices.SortFunc(o, bytes.Compare)
	buf := new(bytes.Buffer)
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
	return errors.Join(
		txn.Set(fstKey, fstData),
		txn.Set(bitmapKey, buf.Bytes()),
	)
}

func ApplyBitDepth(txn *badger.Txn, view string, depth map[uint64]map[uint64]uint64) error {
	b := &v1.FieldViewInfo{}
	key := (&keys.FieldView{}).Key()
	key = append(key, []byte(view)...)
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

func FieldViewInfo(txn *badger.Txn, view string) (*v1.FieldViewInfo, error) {
	key := (&keys.FieldView{}).Key()
	key = append(key, []byte(view)...)
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
