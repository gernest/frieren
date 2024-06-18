package store

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/frieren/internal/keys"
	"github.com/stretchr/testify/require"
)

func TestSequence(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true).WithLogger(nil))
	require.NoError(t, err)
	defer db.Close()

	seq := NewSequence(db)

	v1 := "test_1"
	v2 := "test_2"

	sx := seq.Sequence(v1)
	var next uint64
	for range 5 {
		next = sx.NextID(constants.LastID)
	}

	require.Equal(t, uint64(4), next)

	sx = seq.Sequence(v2)
	for range 5 {
		next = sx.NextID(constants.LastID)
	}
	require.Equal(t, uint64(4), next)

	require.NoError(t, seq.Release())
	t.Run("released sequence is saved", func(t *testing.T) {
		var count, count2 uint64
		err := db.View(func(txn *badger.Txn) error {
			it, err := txn.Get(keys.Seq(new(bytes.Buffer), constants.LastID, v1))
			if err != nil {
				return err
			}
			err = it.Value(func(val []byte) error {
				count = binary.BigEndian.Uint64(val)
				return nil
			})
			if err != nil {
				return err
			}
			it, err = txn.Get(keys.Seq(new(bytes.Buffer), constants.LastID, v2))
			if err != nil {
				return err
			}
			return it.Value(func(val []byte) error {
				count2 = binary.BigEndian.Uint64(val)
				return nil
			})
		})
		require.NoError(t, err)
		require.Equal(t, uint64(5), count)
		require.Equal(t, uint64(5), count2)
	})
}
