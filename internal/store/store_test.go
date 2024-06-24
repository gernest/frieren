package store

import (
	"testing"

	"github.com/gernest/frieren/internal/constants"
	"github.com/stretchr/testify/require"
)

func TestUpsert(t *testing.T) {
	db, err := New(t.TempDir())
	require.NoError(t, err)
	defer db.Close()
	samples := []struct {
		field constants.ID
		value []string
		ids   []uint64
	}{
		{constants.MetricsLabels, []string{"hello", "world"}, []uint64{0, 1}},
		{constants.MetricsLabels, []string{"hello", "world", "foo"}, []uint64{0, 1, 2}},
		{constants.LogsLabels, []string{"key", "value"}, []uint64{0, 1}},
		{constants.LogsLabels, []string{"key", "value"}, []uint64{0, 1}},
		{constants.TracesLabels, []string{"profile", "id"}, []uint64{0, 1}},
		{constants.TracesLabels, []string{"profile", "id"}, []uint64{0, 1}},
	}
	view := "test"
	err = db.Update(func(tx *Tx) error {
		up := tx.Upsert(view)
		for _, v := range samples {
			o := make([]uint64, len(v.ids))
			for i := range v.value {
				o[i] = up.Upsert(v.field, []byte(v.value[i]))
			}
			require.Equal(t, v.ids, o)
		}
		return nil
	})
	require.NoError(t, err)

	err = db.View(func(tx *Tx) error {
		tr := tx.Translate(view)
		for _, v := range samples {
			o := make([]string, len(v.ids))
			for i := range v.ids {
				o[i] = string(tr.Tr(v.field, v.ids[i]))
			}
			require.Equal(t, v.value, o)
		}
		return nil
	})
	require.NoError(t, err)
}
