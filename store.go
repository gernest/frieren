package ernestdb

import (
	"bytes"
	"fmt"
	"runtime"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/gernest/ernestdb/keys"
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
