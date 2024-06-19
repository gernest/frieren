package traces

import (
	"context"
	"encoding/hex"
	"fmt"
	"slices"
	"time"

	v1 "github.com/gernest/frieren/gen/go/fri/v1"
	"github.com/gernest/frieren/internal/blob"
	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/frieren/internal/fields"
	"github.com/gernest/frieren/internal/query"
	"github.com/gernest/frieren/internal/store"
	"github.com/gernest/frieren/internal/traces/predicate"
	"github.com/grafana/tempo/pkg/tempopb"
	commonv1 "github.com/grafana/tempo/pkg/tempopb/common/v1"
	resourcev1 "github.com/grafana/tempo/pkg/tempopb/resource/v1"
	tempov1 "github.com/grafana/tempo/pkg/tempopb/trace/v1"

	"github.com/grafana/tempo/pkg/traceql"
)

type Query struct {
	db *store.Store
}

func (q *Query) FindTraceByID(ctx context.Context, req *tempopb.TraceByIDRequest, timeStart int64, timeEnd int64) (*tempopb.TraceByIDResponse, error) {
	txn := q.db.DB.NewTransaction(false)
	defer txn.Discard()

	tx, err := q.db.Index.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	view, err := query.New(txn, tx, constants.TRACES, time.Unix(0, timeStart), time.Unix(0, timeEnd))
	if err != nil {
		return nil, err
	}
	if view.IsEmpty() {
		return &tempopb.TraceByIDResponse{}, nil
	}

	pre := []predicate.Predicate{
		predicate.NewString(constants.TracesLabels, traceql.OpEqual, "trace:id", hex.EncodeToString(req.TraceID)),
		predicate.NewInt(
			constants.TracesStart, traceql.OpGreaterEqual, uint64(timeStart),
		),
		predicate.NewInt(
			constants.TracesEnd, traceql.OpLessEqual, uint64(timeEnd),
		),
	}
	pre = predicate.Optimize(pre, true)
	all := predicate.And(pre)

	m := make(map[uint64]map[uint64][]*tempov1.Span)
	scope := map[uint64]*commonv1.InstrumentationScope{}
	resource := map[uint64]*resourcev1.Resource{}
	resources := make([]uint64, 0, 64)
	scopes := make([]uint64, 0, 64)

	err = view.Traverse(func(shard *v1.Shard, view string) error {
		clear(m)
		ctx := &predicate.Context{
			Shard: shard.Id,
			View:  view,
			Tx:    tx,
			Txn:   txn,
			Find:  blob.Finder(txn, q.db, view),
			Tr:    blob.Translate(txn, q.db, view),
		}
		r, err := all.Apply(ctx)
		if err != nil {
			return err
		}
		if r.IsEmpty() {
			return nil
		}
		count := r.Count()
		columns := r.Columns()
		mapping := make(map[uint64]int, count)
		var pos int
		r.RangeColumns(func(u uint64) error {
			mapping[u] = pos
			pos++
			return nil
		})
		for i := range columns {
			mapping[columns[i]] = i
		}
		// read contest
		resourceField := fields.New(constants.TracesResource, ctx.Shard, ctx.View)
		scopesField := fields.New(constants.TracesScope, ctx.Shard, ctx.View)
		spansField := fields.New(constants.TracesSpan, ctx.Shard, ctx.View)

		resources = slices.Grow(resources[:0], int(count))[:count]
		err = resourceField.ExtractBSI(ctx.Tx, r, mapping, func(i int, v uint64) error {
			resources[i] = v
			_, ok := m[v]
			if !ok {
				m[v] = make(map[uint64][]*tempov1.Span)
				var x resourcev1.Resource
				err := x.Unmarshal(ctx.Tr(constants.TracesResource, v))
				if err != nil {
					return fmt.Errorf("decoding tempo resource %w", err)
				}
				resource[v] = &x
			}
			return nil
		})
		if err != nil {
			return err
		}
		scopes = slices.Grow(scopes[:0], int(count))[:count]
		err = scopesField.ExtractBSI(ctx.Tx, r, mapping, func(i int, v uint64) error {
			scopes[i] = v
			if len(m[resources[i]][v]) == 0 {
				var x commonv1.InstrumentationScope
				err := x.Unmarshal(ctx.Tr(constants.TracesScope, v))
				if err != nil {
					return fmt.Errorf("decoding tempo scope %w", err)
				}
				scope[v] = &x
			}
			return nil
		})
		if err != nil {
			return err
		}
		return spansField.ExtractBSI(ctx.Tx, r, mapping, func(i int, v uint64) error {
			var x tempov1.Span
			err := x.Unmarshal(ctx.Tr(constants.TracesSpan, v))
			if err != nil {
				return fmt.Errorf("decoding tempo span %w", err)
			}
			m[resources[i]][scopes[i]] = append(m[resources[i]][scopes[i]], &x)
			return nil
		})
	})
	if err != nil {
		return nil, err
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
