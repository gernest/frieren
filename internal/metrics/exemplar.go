package metrics

import (
	"github.com/prometheus/prometheus/prompb"
)

func EncodeExemplars(value []prompb.Exemplar) ([]byte, error) {
	ts := prompb.TimeSeries{Exemplars: value}
	return ts.Marshal()
}
