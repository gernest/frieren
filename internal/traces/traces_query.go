package traces

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/gernest/rbf"
	"github.com/gernest/rbf/dsl/bsi"
	"github.com/gernest/rbf/dsl/mutex"
	rq "github.com/gernest/rbf/dsl/query"
	"github.com/gernest/rbf/dsl/tx"
	"github.com/grafana/tempo/pkg/tempopb"
	commonv1 "github.com/grafana/tempo/pkg/tempopb/common/v1"
	resourcev1 "github.com/grafana/tempo/pkg/tempopb/resource/v1"
	tempov1 "github.com/grafana/tempo/pkg/tempopb/trace/v1"

	"github.com/grafana/tempo/pkg/traceql"
)

func (db *Store) FindTraceByID(ctx context.Context, req *tempopb.TraceByIDRequest, timeStart int64, timeEnd int64) (*tempopb.TraceByIDResponse, error) {
	r, err := db.Reader()
	if err != nil {
		return nil, err
	}
	defer r.Release()

	base := mutex.MatchString{
		Field: "trace_id",
		Op:    mutex.EQ,
		Value: hex.EncodeToString(req.TraceID),
	}
	filter := rq.And{
		bsi.Filter("trace_start_nano", bsi.GE, timeStart, 0),
		bsi.Filter("trace_end_nano", bsi.LE, timeEnd, 0),
	}

	m := make(map[uint64]map[uint64][]*tempov1.Span)
	scope := map[uint64]*commonv1.InstrumentationScope{}
	resource := map[uint64]*resourcev1.Resource{}

	shards := r.Range(time.Unix(0, timeStart), time.Unix(0, timeEnd))
	for i := range shards {
		r.View(shards[i], func(txn *tx.Tx) error {
			r, err := base.Apply(txn, nil)
			if err != nil {
				return err
			}
			if r.IsEmpty() {
				return nil
			}
			f, err := filter.Apply(txn, r)
			if err != nil {
				return err
			}
			if f.IsEmpty() {
				return nil
			}

			// read resource
			resourceMapping := map[uint64]uint64{}
			err = txn.Cursor("resource", func(c *rbf.Cursor, tx *tx.Tx) error {
				return mutex.Extract(c, tx.Shard, f, func(column, value uint64) error {
					if _, seen := resource[value]; seen {
						return nil
					}
					resourceMapping[column] = value
					o := resourcev1.Resource{}
					err := o.Unmarshal(tx.Tr.Blob("resource", value))
					if err != nil {
						return err
					}
					resource[value] = &o
					return nil
				})
			})
			if err != nil {
				return err
			}

			// read scope
			scopeMapping := map[uint64]uint64{}
			err = txn.Cursor("scope", func(c *rbf.Cursor, tx *tx.Tx) error {
				return mutex.Extract(c, tx.Shard, f, func(column, value uint64) error {
					scopeMapping[column] = value
					if _, seen := scope[value]; seen {
						return nil
					}
					o := commonv1.InstrumentationScope{}
					err := o.Unmarshal(tx.Tr.Blob("scope", value))
					if err != nil {
						return err
					}
					scope[value] = &o
					return nil
				})
			})
			if err != nil {
				return err
			}
			// read span
			return txn.Cursor("span", func(c *rbf.Cursor, tx *tx.Tx) error {
				return mutex.Extract(c, tx.Shard, f, func(column, value uint64) error {
					rs, ok := m[resourceMapping[column]]
					if !ok {
						rs = make(map[uint64][]*tempov1.Span)
						m[resourceMapping[column]] = rs
					}
					sid := scopeMapping[column]
					o := tempov1.Span{}
					err := o.Unmarshal(tx.Tr.Blob("scope", value))
					if err != nil {
						return err
					}
					rs[sid] = append(rs[sid], &o)
					return nil
				})
			})
		})
	}

	// Assemble result
	result := &tempopb.TraceByIDResponse{
		Trace: &tempopb.Trace{
			Batches: make([]*tempov1.ResourceSpans, 0, len(resource)),
		},
	}

	for ri, r := range resource {
		xr := &tempov1.ResourceSpans{
			Resource:   r,
			ScopeSpans: make([]*tempov1.ScopeSpans, 0, len(m[ri])),
		}
		for si, sp := range m[ri] {
			xr.ScopeSpans = append(xr.ScopeSpans, &tempov1.ScopeSpans{
				Scope: scope[si],
				Spans: sp,
			})
		}
		result.Trace.Batches = append(result.Trace.Batches, xr)
	}
	return result, nil
}

var _ traceql.SpansetFetcher = (*Store)(nil)

func (s *Store) Fetch(ctx context.Context, req traceql.FetchSpansRequest) (traceql.FetchSpansResponse, error) {
	return traceql.FetchSpansResponse{}, nil
}
