// Contains modified copy of code taken from loki which is under the same
// license as this project.

package api

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/gernest/frieren/internal/logs"
	"github.com/gernest/frieren/internal/store"
	"github.com/grafana/loki/v3/pkg/loghttp"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logqlmodel"
	"github.com/grafana/loki/v3/pkg/querier/queryrange"
	"github.com/grafana/loki/v3/pkg/querier/queryrange/queryrangebase"
	"github.com/grafana/loki/v3/pkg/util/server"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/promql/parser"
)

type Querier interface {
	logql.Querier
	Label(ctx context.Context, req *logproto.LabelRequest) (*logproto.LabelResponse, error)
}

type QueryResponse struct {
	ResultType parser.ValueType `json:"resultType"`
	Result     parser.Value     `json:"result"`
}

type lokiAPI struct {
	querier logql.Querier
	engine  *logql.Engine
}

func newLokiAPI(db *store.Store) *lokiAPI {
	return &lokiAPI{
		querier: logs.NewQuerier(db),
	}
}

func (a *lokiAPI) Register(r *route.Router) {
	wrap := func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		req, err := codec.DecodeRequest(ctx, r, nil)
		if err != nil {
			writeError(w, err)
			return
		}
		rs, err := a.Do(r.Context(), req)
		if err != nil {
			writeError(w, err)
			return
		}
		resp, err := codec.EncodeResponse(ctx, r, rs)
		if err != nil {
			writeError(w, err)
			return
		}
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
	}
	r.Get("/loki/api/v1/query_range", wrap)
	r.Post("/loki/api/v1/query_range", wrap)
	r.Get("/loki/api/v1/query", wrap)
	r.Post("/loki/api/v1/query", wrap)
	r.Get("/loki/api/v1/label", wrap)
	r.Post("/loki/api/v1/label", wrap)
	r.Get("/loki/api/v1/labels", wrap)
	r.Post("/loki/api/v1/labels", wrap)
	r.Get("/loki/api/v1/label/:name/values", wrap)
	r.Post("/loki/api/v1/label/:name/values", wrap)
}

func writeError(w http.ResponseWriter, err error) {
	status, cerr := server.ClientHTTPStatusAndError(err)
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	fmt.Fprint(w, cerr.Error())
}

var codec = queryrange.DefaultCodec

func (q *lokiAPI) RangeQueryHandler(ctx context.Context, req *queryrange.LokiRequest) (logqlmodel.Result, error) {
	params, err := queryrange.ParamsFromRequest(req)
	if err != nil {
		return logqlmodel.Result{}, err
	}

	query := q.engine.Query(params)
	return query.Exec(ctx)
}

func (q *lokiAPI) InstantQueryHandler(ctx context.Context, req *queryrange.LokiInstantRequest) (logqlmodel.Result, error) {
	params, err := queryrange.ParamsFromRequest(req)
	if err != nil {
		return logqlmodel.Result{}, err
	}
	query := q.engine.Query(params)
	return query.Exec(ctx)
}

func (q *lokiAPI) LabelHandler(ctx context.Context, req *logproto.LabelRequest) (*logproto.LabelResponse, error) {
	return nil, nil
}

func (q *lokiAPI) Do(ctx context.Context, req queryrangebase.Request) (queryrangebase.Response, error) {
	switch concrete := req.(type) {
	case *queryrange.LokiRequest:
		res, err := q.RangeQueryHandler(ctx, concrete)
		if err != nil {
			return nil, err
		}

		params, err := queryrange.ParamsFromRequest(req)
		if err != nil {
			return nil, err
		}

		return queryrange.ResultToResponse(res, params)
	case *queryrange.LokiInstantRequest:
		res, err := q.InstantQueryHandler(ctx, concrete)
		if err != nil {
			return nil, err
		}

		params, err := queryrange.ParamsFromRequest(req)
		if err != nil {
			return nil, err
		}

		return queryrange.ResultToResponse(res, params)

	case *queryrange.LabelRequest:
		res, err := q.LabelHandler(ctx, &concrete.LabelRequest)
		if err != nil {
			return nil, err
		}

		return &queryrange.LokiLabelNamesResponse{
			Status:  "success",
			Version: uint32(loghttp.VersionV1),
			Data:    res.Values,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported query type %T", req)
	}
}

func (a *lokiAPI) Handler() http.Handler {
	return queryrange.NewSerializeHTTPHandler(a, queryrange.DefaultCodec)
}
