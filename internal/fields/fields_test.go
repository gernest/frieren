package fields

import (
	"testing"

	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/frieren/internal/ro"
	"github.com/gernest/rbf"
	"github.com/gernest/roaring"
	"github.com/stretchr/testify/require"
)

func TestSets(t *testing.T) {
	db := rbf.NewDB(t.TempDir(), nil)
	require.NoError(t, db.Open())
	defer db.Close()

	tx, err := db.Begin(true)
	require.NoError(t, err)
	sample := []struct {
		row    uint64
		values []uint64
	}{
		{1, []uint64{100, 200, 300}},
		{2, []uint64{400, 500, 600}},
	}
	b := roaring.NewBitmap()
	e := roaring.NewBitmap()
	for _, v := range sample {
		ro.Set(b, e, v.row, v.values)
	}
	f := New(constants.LastID, 0, "test")
	tx.AddRoaring(f.String(), b)
	tx.AddRoaring(f.ExistView(), e)

	require.NoError(t, tx.Commit())
	tx, err = db.Begin(false)
	require.NoError(t, err)
	defer tx.Rollback()

	ex, err := f.ExistsSet(tx)
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2}, ex.Columns())

	r, err := f.Row(tx, 100)
	require.NoError(t, err)
	require.Equal(t, []uint64{1}, r.Columns())

	rs, err := f.Rows(tx, 0, roaring.NewBitmapColumnFilter(1))
	require.NoError(t, err)
	require.Equal(t, []uint64{100, 200, 300}, rs)

}
