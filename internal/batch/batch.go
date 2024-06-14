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
	buf := new(bytes.Buffer)
	for shard, bm := range data {
		err := updateFST(buf, txn, tr, shard, view, id, bm)
		if err != nil {
			return fmt.Errorf("inserting exists bsi %w", err)
		}
	}
	return txn.Commit()
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
	return errors.Join(
		txn.Set(fstKey, fstData),
		txn.Set(bitmapKey, buf.Bytes()),
	)
}

func ApplyBitDepth(txn *badger.Txn, view string, depth map[uint64]map[uint64]uint64) error {
	b := &v1.FieldViewInfo{}
	key := keys.FieldView(new(bytes.Buffer), view)
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
	key := keys.FieldView(new(bytes.Buffer), view)
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
