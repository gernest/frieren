package ernestdb

import (
	"testing"

	"github.com/RoaringBitmap/roaring/roaring64"
)

func TestYay(t *testing.T) {
	b := roaring64.NewDefaultBSI()
	b.SetValue(1, 10)
	b.SetValue(2, 11)
	b.SetValue(3, 12)
	b.SetValue(4, 12)

	t.Error(b.Transpose().ToArray())
}
