// Contains modified copy of code taken from tempo which is under the same
// license as this project.
package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"github.com/gernest/frieren/internal/store"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/grafana/tempo/pkg/api"
	"github.com/grafana/tempo/pkg/model/trace"
	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/pkg/util"
	"github.com/prometheus/common/route"
)

type Engine interface {
	FindTraceByID(ctx context.Context, req *tempopb.TraceByIDRequest, timeStart int64, timeEnd int64) (*tempopb.TraceByIDResponse, error)
	SearchRecent(ctx context.Context, req *tempopb.SearchRequest) (*tempopb.SearchResponse, error)
	SearchTags(ctx context.Context, req *tempopb.SearchTagsRequest) (*tempopb.SearchTagsResponse, error)
	SearchTagValues(ctx context.Context, req *tempopb.SearchTagValuesRequest) (*tempopb.SearchTagValuesResponse, error)
}

type tempoAPI struct {
	engine Engine
}

func newTempoAPI(db *store.Store) *tempoAPI {
	return &tempoAPI{}
}

func (a *tempoAPI) Register(r *route.Router) {
	r.Get("/api/traces/:id", a.findTraceByID)
	r.Post("/api/traces/:id", a.findTraceByID)
	r.Get(api.PathSearch, a.search)
	r.Post(api.PathSearch, a.search)
	r.Get(api.PathSearchTags, a.searchTags)
	r.Post(api.PathSearchTags, a.searchTags)
	r.Get("/api/search/tag/:name/values", a.searchTagValues)
	r.Post("/api/search/tag/:name/values", a.searchTagValues)
}

func (a *tempoAPI) findTraceByID(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	byteID, err := parseTraceID(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	blockStart, blockEnd, queryMode, timeStart, timeEnd, err := api.ValidateAndSanitizeRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	resp, err := a.engine.FindTraceByID(ctx, &tempopb.TraceByIDRequest{
		TraceID:    byteID,
		BlockStart: blockStart,
		BlockEnd:   blockEnd,
		QueryMode:  queryMode,
	}, timeStart, timeEnd)
	if err != nil {
		handleError(w, err)
		return
	}
	// record not found here, but continue on so we can marshal metrics
	// to the body
	if resp.Trace == nil || len(resp.Trace.Batches) == 0 {
		w.WriteHeader(http.StatusNotFound)
	}
	marshaller := &jsonpb.Marshaler{}
	err = marshaller.Marshal(w, resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set(api.HeaderContentType, api.HeaderAcceptJSON)
}

func (a *tempoAPI) search(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := api.ParseSearchRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	resp, err := a.engine.SearchRecent(ctx, req)
	if err != nil {
		handleError(w, err)
		return
	}

	marshaller := &jsonpb.Marshaler{}
	err = marshaller.Marshal(w, resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set(api.HeaderContentType, api.HeaderAcceptJSON)
}

func (a *tempoAPI) searchTags(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := api.ParseSearchTagsRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	resp, err := a.engine.SearchTags(ctx, req)
	if err != nil {
		handleError(w, err)
		return
	}

	marshaller := &jsonpb.Marshaler{}
	err = marshaller.Marshal(w, resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set(api.HeaderContentType, api.HeaderAcceptJSON)
}

func (a *tempoAPI) searchTagValues(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := parseSearchTagValuesRequest(r, true)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp, err := a.engine.SearchTagValues(ctx, req)
	if err != nil {
		handleError(w, err)
		return
	}
	marshaller := &jsonpb.Marshaler{}
	err = marshaller.Marshal(w, resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set(api.HeaderContentType, api.HeaderAcceptJSON)
}

func parseTraceID(ctx context.Context) ([]byte, error) {
	traceID := route.Param(ctx, "id")
	if traceID == "" {
		return nil, fmt.Errorf("please provide a traceID")
	}
	return util.HexStringToTraceID(traceID)
}

func handleError(w http.ResponseWriter, err error) {
	if err == nil {
		return
	}

	if errors.Is(err, context.Canceled) {
		// todo: context is also canceled when we hit the query timeout. research what the behavior is
		// ignore this error. we regularly cancel context once queries are complete
		return
	}

	// todo: better understand all errors returned from queriers and categorize more as 4XX
	if errors.Is(err, trace.ErrTraceTooLarge) {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	http.Error(w, err.Error(), http.StatusInternalServerError)
}

const (
	// search
	urlParamQuery = "q"
	urlParamStart = "start"
	urlParamEnd   = "end"
)

func parseSearchTagValuesRequest(r *http.Request, enforceTraceQL bool) (*tempopb.SearchTagValuesRequest, error) {
	escapedTagName := route.Param(r.Context(), "name")
	if escapedTagName == "" {
		return nil, errors.New("please provide a tagName")
	}

	if escapedTagName == "" {
		return nil, errors.New("please provide a non-empty tagName")
	}

	tagName, unescapingError := url.QueryUnescape(escapedTagName)
	if unescapingError != nil {
		return nil, errors.New("error in unescaping tagName")
	}

	if enforceTraceQL {
		_, err := traceql.ParseIdentifier(tagName)
		if err != nil {
			return nil, fmt.Errorf("please provide a valid tagName: %w", err)
		}
	}
	params := r.URL.Query()

	query := params.Get(urlParamQuery)

	req := &tempopb.SearchTagValuesRequest{
		TagName: tagName,
		Query:   query,
	}

	if s := params.Get(urlParamStart); s != "" {
		start, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid start: %w", err)
		}
		req.Start = uint32(start)
	}

	if s := params.Get(urlParamEnd); s != "" {
		end, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid end: %w", err)
		}
		req.End = uint32(end)
	}

	return req, nil
}
