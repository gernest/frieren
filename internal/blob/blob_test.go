package blob

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/frieren/internal/store"
	"github.com/stretchr/testify/require"
)

func TestUpsert(t *testing.T) {
	db, err := store.New(t.TempDir())
	require.NoError(t, err)
	defer db.Close()
	samples := []struct {
		field constants.ID
		value []string
		ids   []uint64
	}{
		{constants.MetricsFST, []string{"hello", "world"}, []uint64{0, 1}},
		{constants.MetricsFST, []string{"hello", "world", "foo"}, []uint64{0, 1, 2}},
		{constants.LogsFST, []string{"key", "value"}, []uint64{0, 1}},
		{constants.LogsFST, []string{"key", "value"}, []uint64{0, 1}},
		{constants.TracesFST, []string{"profile", "id"}, []uint64{0, 1}},
		{constants.TracesFST, []string{"profile", "id"}, []uint64{0, 1}},
	}
	view := "test"
	seq := db.Seq.Sequence(view)
	err = db.DB.Update(func(txn *badger.Txn) error {
		up := Upsert(txn, db, seq, view)
		for _, v := range samples {
			o := make([]uint64, len(v.ids))
			for i := range v.value {
				o[i] = up(v.field, []byte(v.value[i]))
			}
			require.Equal(t, v.ids, o)
		}
		return nil
	})
	require.NoError(t, err)
}
