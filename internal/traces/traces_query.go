package traces

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/gernest/frieren/internal/lbx"
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

	otlpScope := map[uint64]*commonv1.InstrumentationScope{}
	otlpResourrce := map[uint64]*resourcev1.Resource{}

	err = r.View(func(txn *tx.Tx) error {
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
		resource, err := txn.Get("resource")
		if err != nil {
			return err
		}
		defer resource.Close()
		scope, err := txn.Get("scope")
		if err != nil {
			return err
		}
		defer scope.Close()

		span, err := txn.Get("span")
		if err != nil {
			return err
		}
		defer span.Close()
		columns := f.Columns()
		data := lbx.NewData(columns)

		// read resource
		resourceMapping := map[uint64]uint64{}
		err = lbx.BSI(data, columns, resource, f, txn.Shard(), func(position int, value int64) error {
			resourceMapping[columns[position]] = uint64(value)
			if _, seen := otlpResourrce[uint64(value)]; seen {
				return nil
			}
			o := resourcev1.Resource{}
			err := o.Unmarshal(txn.Blob("resource", uint64(value)))
			if err != nil {
				return fmt.Errorf("decoding resource %w", err)
			}
			otlpResourrce[uint64(value)] = &o
			return nil
		})
		if err != nil {
			return err
		}

		// read scope
		scopeMapping := map[uint64]uint64{}
		err = lbx.BSI(data, columns, scope, f, txn.Shard(), func(position int, value int64) error {
			scopeMapping[columns[position]] = uint64(value)
			if _, seen := otlpScope[uint64(value)]; seen {
				return nil
			}
			o := commonv1.InstrumentationScope{}
			err := o.Unmarshal(txn.Blob("scope", uint64(value)))
			if err != nil {
				return fmt.Errorf("decoding scope %w", err)
			}
			otlpScope[uint64(value)] = &o
			return nil
		})
		if err != nil {
			return err
		}
		return lbx.BSI(data, columns, span, f, txn.Shard(), func(position int, value int64) error {
			column := columns[position]
			rs, ok := m[resourceMapping[column]]
			if !ok {
				rs = make(map[uint64][]*tempov1.Span)
				m[resourceMapping[column]] = rs
			}
			sid := scopeMapping[column]
			o := tempov1.Span{}
			err := o.Unmarshal(txn.Blob("span", uint64(value)))
			if err != nil {
				return fmt.Errorf("decoding span %w", err)
			}
			rs[sid] = append(rs[sid], &o)
			return nil
		})
	})

	if err != nil {
		return nil, err
	}

	// Assemble result
	result := &tempopb.TraceByIDResponse{
		Trace: &tempopb.Trace{
			Batches: make([]*tempov1.ResourceSpans, 0, len(otlpResourrce)),
		},
	}

	for ri, r := range otlpResourrce {
		xr := &tempov1.ResourceSpans{
			Resource:   r,
			ScopeSpans: make([]*tempov1.ScopeSpans, 0, len(m[ri])),
		}
		for si, sp := range m[ri] {
			xr.ScopeSpans = append(xr.ScopeSpans, &tempov1.ScopeSpans{
				Scope: otlpScope[si],
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
