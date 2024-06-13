package batch

import (
	"bytes"
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

func ApplyShards(tx *rbf.Tx, view *fields.Fragment, m *roaring64.Bitmap) error {
	key := view.WithShard(0).String()
	_, err := tx.Add(key, m.ToArray()...)
	if err != nil {
		return fmt.Errorf("adding to %s %w", key, err)
	}
	return nil
}

func ApplyFST(txn *badger.Txn, tx *rbf.Tx, tr blob.Tr, fra, fstBitmap *fields.Fragment, data map[uint64]*roaring64.Bitmap) error {
	for shard := range data {
		fst := fra.WithShard(shard)
		fstBm := fstBitmap.WithShard(shard)
		err := updateFST(txn, tx, tr, fst, fstBm)
		if err != nil {
			return fmt.Errorf("inserting exists bsi %w", err)
		}
	}
	return txn.Commit()
}

func updateFST(txn *badger.Txn, tx *rbf.Tx, tr blob.Tr, fra, bitmapFra *fields.Fragment) error {
	r, err := tx.RoaringBitmap(bitmapFra.String())
	if err != nil {
		return fmt.Errorf("reading fst bitmap %w", err)
	}
	o := make([][]byte, 0, r.Count())
	itr := r.Iterator()
	itr.Seek(0)
	for v, eof := itr.Next(); !eof; v, eof = itr.Next() {
		err := tr(constants.MetricsFST, v, func(val []byte) error {
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
	return txn.Set([]byte(fra.String()), buf.Bytes())
}

func UpsertBitDepth(txn *badger.Txn, depth map[uint64]map[uint64]uint64) error {
	o := (&keys.BitDepth{}).Slice()
	b := &v1.BitDepth{}
	buf := make([]byte, 0, len(o)*8)
	for shard, set := range depth {
		b.Reset()
		o[len(o)-1] = shard
		key := keys.Encode(buf, o)
		store.Get(txn, key, func(val []byte) error {
			return proto.Unmarshal(val, b)
		})
		if b.BitDepth == nil {
			b.BitDepth = set
		} else {
			for k, v := range set {
				b.BitDepth[k] = max(b.BitDepth[k], v)
			}
		}
		data, err := proto.Marshal(b)
		if err != nil {
			return err
		}
		err = txn.Set(bytes.Clone(key), data)
		if err != nil {
			return err
		}
	}
	return nil
}
