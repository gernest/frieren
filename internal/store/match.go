package store

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/prometheus/prometheus/model/labels"
	"go.etcd.io/bbolt"
)

func (db *DB) Names() (names []string, err error) {
	err = db.db.View(func(tx *bbolt.Tx) error {
		lbl := tx.Bucket(labelsBucket)
		if lbl == nil {
			return nil
		}
		return lbl.ForEachBucket(func(k []byte) error {
			names = append(names, string(k))
			return nil
		})
	})
	return
}

func (db *DB) Values(name string) (values []string, err error) {
	err = db.db.View(func(tx *bbolt.Tx) error {
		lbl := tx.Bucket(labelsBucket)
		key := lbl.Bucket([]byte(name))
		if key == nil {
			return nil
		}
		return key.ForEachBucket(func(k []byte) error {
			values = append(values, string(k))
			return nil
		})
	})
	return
}

func (db *DB) Match(w *roaring.Bitmap, matchers ...*labels.Matcher) error {
	yes, no := separate(matchers)
	if len(yes) == 0 && len(no) > 0 {
		return nil
	}
	return db.db.View(func(tx *bbolt.Tx) error {
		lbl := tx.Bucket(labelsBucket)

		a := roaring.New()
		for i := range yes {
			x, err := match(lbl, yes[i])
			if err != nil {
				return err
			}
			if x.IsEmpty() {
				return nil
			}
			if i == 0 {
				a = x
			} else {
				a.And(x)
			}
			if a.IsEmpty() {
				return nil
			}
		}
		if a.IsEmpty() {
			return nil
		}
		for i := range no {
			switch no[i].Type {
			case labels.MatchNotEqual:
				no[i].Type = labels.MatchEqual
				x, err := match(lbl, no[i])
				if err != nil {
					return err
				}
				a.AndNot(x)
				if a.IsEmpty() {
					return nil
				}
			case labels.MatchNotRegexp:
				no[i].Type = labels.MatchRegexp
				x, err := match(lbl, no[i])
				if err != nil {
					return err
				}
				a.AndNot(x)
				if a.IsEmpty() {
					return nil
				}
			}

		}
		w.Or(a)
		return nil
	})
}

func separate(m []*labels.Matcher) (yes, no []*labels.Matcher) {
	for _, v := range m {
		switch v.Type {
		case labels.MatchNotEqual, labels.MatchNotRegexp:
			no = append(no, v)
		default:
			yes = append(yes, v)
		}
	}
	return
}
func match(tx *bbolt.Bucket, m *labels.Matcher) (*roaring.Bitmap, error) {
	key := tx.Bucket([]byte(m.Name))
	if key == nil {
		return roaring.New(), nil
	}

	switch m.Type {
	case labels.MatchEqual:
		value := key.Bucket([]byte(m.Value))
		if value == nil {
			return roaring.New(), nil
		}
		r := roaring.New()
		err := r.UnmarshalBinary(value.Get(root))
		if err != nil {
			return nil, err
		}

		return r, nil
	case labels.MatchRegexp:
		b := roaring.New()
		o := roaring.New()
		err := key.ForEachBucket(func(k []byte) error {
			if !m.Matches(string(k)) {
				return nil
			}
			o.Clear()
			err := o.UnmarshalBinary(key.Bucket(k).Get(root))
			if err != nil {
				return err
			}
			b.Or(o)
			return nil
		})
		if err != nil {
			return nil, err
		}
		return b, nil
	default:
		return roaring.New(), nil
	}
}
