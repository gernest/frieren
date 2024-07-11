// Copyright 2016 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"time"

	"github.com/gernest/frieren/internal/logs"
	"github.com/gernest/frieren/internal/metrics"
	"github.com/gernest/frieren/internal/self"
	"github.com/gernest/frieren/internal/traces"
	"github.com/go-kit/log"
	"github.com/grafana/regexp"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/promlog"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/relabel"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	ps "github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/prometheus/prometheus/util/httputil"
	"github.com/prometheus/prometheus/util/stats"
)

var (
	// MinTime is the default timestamp used for the begin of optional time ranges.
	// Exposed to let downstream projects to reference it.
	MinTime = time.Unix(math.MinInt64/1000+62135596801, 0).UTC()

	// MaxTime is the default timestamp used for the end of optional time ranges.
	// Exposed to let downstream projects to reference it.
	MaxTime = time.Unix(math.MaxInt64/1000-62135596801, 999999999).UTC()

	minTimeFormatted = MinTime.Format(time.RFC3339Nano)
	maxTimeFormatted = MaxTime.Format(time.RFC3339Nano)
)

type status string

const (
	statusSuccess status = "success"
	statusError   status = "error"

	// Non-standard status code (originally introduced by nginx) for the case when a client closes
	// the connection while the server is still processing the request.
	statusClientClosedConnection = 499
)

type errorType string

const (
	errorNone          errorType = ""
	errorTimeout       errorType = "timeout"
	errorCanceled      errorType = "canceled"
	errorExec          errorType = "execution"
	errorBadData       errorType = "bad_data"
	errorInternal      errorType = "internal"
	errorUnavailable   errorType = "unavailable"
	errorNotFound      errorType = "not_found"
	errorNotAcceptable errorType = "not_acceptable"
)

// Response contains a response to a HTTP API request.
type Response struct {
	Status    status      `json:"status"`
	Data      interface{} `json:"data,omitempty"`
	ErrorType errorType   `json:"errorType,omitempty"`
	Error     string      `json:"error,omitempty"`
	Warnings  []string    `json:"warnings,omitempty"`
}

type apiError struct {
	typ errorType
	err error
}

func (e *apiError) Error() string {
	return fmt.Sprintf("%s: %s", e.typ, e.err)
}

type apiFuncResult struct {
	data      interface{}
	err       *apiError
	warnings  annotations.Annotations
	finalizer func()
}

type apiFunc func(r *http.Request) apiFuncResult

type prometheusAPI struct {
	qe       *promql.Engine
	qs       ps.Queryable
	examplar ps.ExemplarQueryable
	db       *metrics.Store
	now      func() time.Time
}

func Add(mux *http.ServeMux, mdb *metrics.Store, tdb *traces.Store, ldb *logs.Store) {
	api := route.New()
	newPrometheusAPI(mdb).Register(api)
	newLokiAPI(ldb).Register(api)
	newTempoAPI(tdb).Register(api)
	cors, _ := compileCORSRegexString(".*")
	mux.Handle("/api/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		httputil.SetCORS(w, cors, r)
		api.ServeHTTP(w, r)
	}))
}

func newPrometheusAPI(db *metrics.Store) *prometheusAPI {

	query := db.Queryable()

	trackPath := filepath.Join(db.Path(), "prometheus", "track")
	os.MkdirAll(trackPath, 0755)
	logger := promlog.New(&promlog.Config{})

	eo := promql.EngineOpts{
		Logger:                   log.With(logger, "component", "query engine"),
		MaxSamples:               50000000,
		Reg:                      prometheus.DefaultRegisterer,
		Timeout:                  2 * time.Minute,
		ActiveQueryTracker:       promql.NewActiveQueryTracker(trackPath, 20, log.With(logger, "component", "activeQueryTracker")),
		LookbackDelta:            5 * time.Minute,
		NoStepSubqueryIntervalFn: func(rangeMillis int64) int64 { return time.Minute.Milliseconds() },
		EnableAtModifier:         true,
		EnableNegativeOffset:     true,
	}
	queryEngine := promql.NewEngine(eo)
	return &prometheusAPI{
		db:       db,
		qe:       queryEngine,
		qs:       query,
		examplar: db,
		now:      func() time.Time { return time.Now().UTC() },
	}
}

func compileCORSRegexString(s string) (*regexp.Regexp, error) {
	r, err := relabel.NewRegexp(s)
	if err != nil {
		return nil, err
	}
	return r.Regexp, nil
}

func (api *prometheusAPI) Register(r *route.Router) {
	wrap := func(f apiFunc) http.HandlerFunc {
		hf := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			result := f(r)
			if result.finalizer != nil {
				defer result.finalizer()
			}
			if result.err != nil {
				api.respondError(w, result.err, result.data)
				return
			}

			if result.data != nil {
				api.respond(w, r, result.data)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		})
		return httputil.CompressionHandler{
			Handler: hf,
		}.ServeHTTP
	}
	r.Get("/api/v1/query", wrap(api.query))
	r.Post("/api/v1/query", wrap(api.query))
	r.Get("/api/v1/query_range", wrap(api.queryRange))
	r.Post("/api/v1/query_range", wrap(api.queryRange))
	r.Get("/api/v1/query_exemplars", wrap(api.queryExemplars))
	r.Post("/api/v1/query_exemplars", wrap(api.queryExemplars))

	r.Get("/api/v1/labels", wrap(api.labelNames))
	r.Post("/api/v1/labels", wrap(api.labelNames))
	r.Get("/api/v1/label/:name/values", wrap(api.labelValues))
	r.Get("/api/v1/series", wrap(api.series))
	r.Post("/api/v1/series", wrap(api.series))
	r.Get("/api/v1/status/buildinfo", wrap(api.buildInfo))

	r.Get("/api/v1/metadata", wrap(api.metricMetadata))
}

type QueryData struct {
	ResultType parser.ValueType `json:"resultType"`
	Result     parser.Value     `json:"result"`
	Stats      stats.QueryStats `json:"stats,omitempty"`
}

func invalidParamError(err error, parameter string) apiFuncResult {
	return apiFuncResult{nil, &apiError{
		errorBadData, fmt.Errorf("invalid parameter %q: %w", parameter, err),
	}, nil, nil}
}

func (api *prometheusAPI) query(r *http.Request) (result apiFuncResult) {
	ctx := r.Context()
	ctx, span := self.Start(ctx, "PROMETHEUS.query")
	defer span.End()
	ts, err := parseTimeParam(r, "time", api.now())
	if err != nil {
		return invalidParamError(err, "time")
	}
	if to := r.FormValue("timeout"); to != "" {
		var cancel context.CancelFunc
		timeout, err := parseDuration(to)
		if err != nil {
			return invalidParamError(err, "timeout")
		}

		ctx, cancel = context.WithDeadline(ctx, api.now().Add(timeout))
		defer cancel()
	}

	opts, err := extractQueryOpts(r)
	if err != nil {
		return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
	}
	qry, err := api.qe.NewInstantQuery(ctx, api.qs, opts, r.FormValue("query"), ts)
	if err != nil {
		return invalidParamError(err, "query")
	}

	// From now on, we must only return with a finalizer in the result (to
	// be called by the caller) or call qry.Close ourselves (which is
	// required in the case of a panic).
	defer func() {
		if result.finalizer == nil {
			qry.Close()
		}
	}()

	ctx = httputil.ContextFromRequest(ctx, r)

	res := qry.Exec(ctx)
	if res.Err != nil {
		return apiFuncResult{nil, returnAPIError(res.Err), res.Warnings, qry.Close}
	}

	return apiFuncResult{&QueryData{
		ResultType: res.Value.Type(),
		Result:     res.Value,
		Stats:      defaultStatsRenderer(qry.Stats(), r.FormValue("stats")),
	}, nil, res.Warnings, qry.Close}
}

func (api *prometheusAPI) queryRange(r *http.Request) (result apiFuncResult) {
	ctx := r.Context()
	ctx, span := self.Start(ctx, "PROMETHEUS.queryRange")
	defer span.End()

	start, err := parseTime(r.FormValue("start"))
	if err != nil {
		return invalidParamError(err, "start")
	}
	end, err := parseTime(r.FormValue("end"))
	if err != nil {
		return invalidParamError(err, "end")
	}
	if end.Before(start) {
		return invalidParamError(errors.New("end timestamp must not be before start time"), "end")
	}

	step, err := parseDuration(r.FormValue("step"))
	if err != nil {
		return invalidParamError(err, "step")
	}

	if step <= 0 {
		return invalidParamError(errors.New("zero or negative query resolution step widths are not accepted. Try a positive integer"), "step")
	}

	// For safety, limit the number of returned points per timeseries.
	// This is sufficient for 60s resolution for a week or 1h resolution for a year.
	if end.Sub(start)/step > 11000 {
		err := errors.New("exceeded maximum resolution of 11,000 points per timeseries. Try decreasing the query resolution (?step=XX)")
		return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
	}

	if to := r.FormValue("timeout"); to != "" {
		var cancel context.CancelFunc
		timeout, err := parseDuration(to)
		if err != nil {
			return invalidParamError(err, "timeout")
		}

		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	opts, err := extractQueryOpts(r)
	if err != nil {
		return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
	}
	qry, err := api.qe.NewRangeQuery(ctx, api.qs, opts, r.FormValue("query"), start, end, step)
	if err != nil {
		return invalidParamError(err, "query")
	}
	// From now on, we must only return with a finalizer in the result (to
	// be called by the caller) or call qry.Close ourselves (which is
	// required in the case of a panic).
	defer func() {
		if result.finalizer == nil {
			qry.Close()
		}
	}()

	ctx = httputil.ContextFromRequest(ctx, r)

	res := qry.Exec(ctx)
	if res.Err != nil {
		return apiFuncResult{nil, returnAPIError(res.Err), res.Warnings, qry.Close}
	}

	return apiFuncResult{&QueryData{
		ResultType: res.Value.Type(),
		Result:     res.Value,
		Stats:      defaultStatsRenderer(qry.Stats(), r.FormValue("stats")),
	}, nil, res.Warnings, qry.Close}
}

func (api *prometheusAPI) queryExemplars(r *http.Request) apiFuncResult {
	start, err := parseTimeParam(r, "start", MinTime)
	if err != nil {
		return invalidParamError(err, "start")
	}
	end, err := parseTimeParam(r, "end", MaxTime)
	if err != nil {
		return invalidParamError(err, "end")
	}
	if end.Before(start) {
		err := errors.New("end timestamp must not be before start timestamp")
		return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
	}

	expr, err := parser.ParseExpr(r.FormValue("query"))
	if err != nil {
		return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
	}

	selectors := parser.ExtractSelectors(expr)
	if len(selectors) < 1 {
		return apiFuncResult{nil, nil, nil, nil}
	}

	ctx := r.Context()
	eq, err := api.examplar.ExemplarQuerier(ctx)
	if err != nil {
		return apiFuncResult{nil, returnAPIError(err), nil, nil}
	}

	res, err := eq.Select(timestamp.FromTime(start), timestamp.FromTime(end), selectors...)
	if err != nil {
		return apiFuncResult{nil, returnAPIError(err), nil, nil}
	}

	return apiFuncResult{res, nil, nil, nil}
}

func (api *prometheusAPI) metricMetadata(r *http.Request) apiFuncResult {
	metric := r.FormValue("metric")
	meta, err := api.db.Metadata(metric)
	if err != nil {
		return apiFuncResult{nil, returnAPIError(err), nil, nil}
	}
	return apiFuncResult{meta, nil, nil, nil}
}

func defaultStatsRenderer(s *stats.Statistics, param string) stats.QueryStats {
	if param != "" {
		return stats.NewQueryStats(s)
	}
	return nil
}

func returnAPIError(err error) *apiError {
	if err == nil {
		return nil
	}

	var eqc promql.ErrQueryCanceled
	var eqt promql.ErrQueryTimeout
	var es promql.ErrStorage
	switch {
	case errors.As(err, &eqc):
		return &apiError{errorCanceled, err}
	case errors.As(err, &eqt):
		return &apiError{errorTimeout, err}
	case errors.As(err, &es):
		return &apiError{errorInternal, err}
	}

	if errors.Is(err, context.Canceled) {
		return &apiError{errorCanceled, err}
	}

	return &apiError{errorExec, err}
}

func (api *prometheusAPI) labelNames(r *http.Request) apiFuncResult {
	ctx := r.Context()
	_, span := self.Start(ctx, "PROMETHEUS.labelNames")
	defer span.End()

	limit, err := parseLimitParam(r.FormValue("limit"))
	if err != nil {
		return invalidParamError(err, "limit")
	}

	start, err := parseTimeParam(r, "start", MinTime)
	if err != nil {
		return invalidParamError(err, "start")
	}
	end, err := parseTimeParam(r, "end", MaxTime)
	if err != nil {
		return invalidParamError(err, "end")
	}

	matcherSets, err := parseMatchersParam(r.Form["match[]"])
	if err != nil {
		return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
	}

	q, err := api.qs.Querier(timestamp.FromTime(start), timestamp.FromTime(end))
	if err != nil {
		return apiFuncResult{nil, returnAPIError(err), nil, nil}
	}
	defer q.Close()

	var (
		names    []string
		warnings annotations.Annotations
	)
	if len(matcherSets) > 1 {
		labelNamesSet := make(map[string]struct{})

		for _, matchers := range matcherSets {
			vals, callWarnings, err := q.LabelNames(r.Context(), matchers...)
			if err != nil {
				return apiFuncResult{nil, returnAPIError(err), warnings, nil}
			}

			warnings.Merge(callWarnings)
			for _, val := range vals {
				labelNamesSet[val] = struct{}{}
			}
		}

		// Convert the map to an array.
		names = make([]string, 0, len(labelNamesSet))
		for key := range labelNamesSet {
			names = append(names, key)
		}
		slices.Sort(names)
	} else {
		var matchers []*labels.Matcher
		if len(matcherSets) == 1 {
			matchers = matcherSets[0]
		}
		names, warnings, err = q.LabelNames(r.Context(), matchers...)
		if err != nil {
			return apiFuncResult{nil, &apiError{errorExec, err}, warnings, nil}
		}
	}

	if names == nil {
		names = []string{}
	}

	if len(names) >= limit {
		names = names[:limit]
		warnings = warnings.Add(errors.New("results truncated due to limit"))
	}
	return apiFuncResult{names, nil, warnings, nil}
}

func (api *prometheusAPI) labelValues(r *http.Request) (result apiFuncResult) {
	ctx := r.Context()
	ctx, span := self.Start(ctx, "PROMETHEUS.labelValues")
	defer span.End()

	name := route.Param(ctx, "name")

	if !model.LabelNameRE.MatchString(name) {
		return apiFuncResult{nil, &apiError{errorBadData, fmt.Errorf("invalid label name: %q", name)}, nil, nil}
	}

	limit, err := parseLimitParam(r.FormValue("limit"))
	if err != nil {
		return invalidParamError(err, "limit")
	}

	start, err := parseTimeParam(r, "start", MinTime)
	if err != nil {
		return invalidParamError(err, "start")
	}
	end, err := parseTimeParam(r, "end", MaxTime)
	if err != nil {
		return invalidParamError(err, "end")
	}

	matcherSets, err := parseMatchersParam(r.Form["match[]"])
	if err != nil {
		return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
	}

	q, err := api.qs.Querier(timestamp.FromTime(start), timestamp.FromTime(end))
	if err != nil {
		return apiFuncResult{nil, &apiError{errorExec, err}, nil, nil}
	}
	// From now on, we must only return with a finalizer in the result (to
	// be called by the caller) or call q.Close ourselves (which is required
	// in the case of a panic).
	defer func() {
		if result.finalizer == nil {
			q.Close()
		}
	}()
	closer := func() {
		q.Close()
	}

	var (
		vals     []string
		warnings annotations.Annotations
	)
	if len(matcherSets) > 1 {
		var callWarnings annotations.Annotations
		labelValuesSet := make(map[string]struct{})
		for _, matchers := range matcherSets {
			vals, callWarnings, err = q.LabelValues(ctx, name, matchers...)
			if err != nil {
				return apiFuncResult{nil, &apiError{errorExec, err}, warnings, closer}
			}
			warnings.Merge(callWarnings)
			for _, val := range vals {
				labelValuesSet[val] = struct{}{}
			}
		}

		vals = make([]string, 0, len(labelValuesSet))
		for val := range labelValuesSet {
			vals = append(vals, val)
		}
	} else {
		var matchers []*labels.Matcher
		if len(matcherSets) == 1 {
			matchers = matcherSets[0]
		}
		vals, warnings, err = q.LabelValues(ctx, name, matchers...)
		if err != nil {
			return apiFuncResult{nil, &apiError{errorExec, err}, warnings, closer}
		}

		if vals == nil {
			vals = []string{}
		}
	}

	slices.Sort(vals)

	if len(vals) >= limit {
		vals = vals[:limit]
		warnings = warnings.Add(errors.New("results truncated due to limit"))
	}

	return apiFuncResult{vals, nil, warnings, closer}
}

func (api *prometheusAPI) series(r *http.Request) (result apiFuncResult) {
	ctx := r.Context()
	ctx, span := self.Start(ctx, "PROMETHEUS.series")
	defer span.End()

	if err := r.ParseForm(); err != nil {
		return apiFuncResult{nil, &apiError{errorBadData, fmt.Errorf("error parsing form values: %w", err)}, nil, nil}
	}
	if len(r.Form["match[]"]) == 0 {
		return apiFuncResult{nil, &apiError{errorBadData, errors.New("no match[] parameter provided")}, nil, nil}
	}

	limit, err := parseLimitParam(r.FormValue("limit"))
	if err != nil {
		return invalidParamError(err, "limit")
	}

	start, err := parseTimeParam(r, "start", MinTime)
	if err != nil {
		return invalidParamError(err, "start")
	}
	end, err := parseTimeParam(r, "end", MaxTime)
	if err != nil {
		return invalidParamError(err, "end")
	}

	matcherSets, err := parseMatchersParam(r.Form["match[]"])
	if err != nil {
		return invalidParamError(err, "match[]")
	}

	q, err := api.qs.Querier(timestamp.FromTime(start), timestamp.FromTime(end))
	if err != nil {
		return apiFuncResult{nil, returnAPIError(err), nil, nil}
	}
	// From now on, we must only return with a finalizer in the result (to
	// be called by the caller) or call q.Close ourselves (which is required
	// in the case of a panic).
	defer func() {
		if result.finalizer == nil {
			q.Close()
		}
	}()
	closer := func() {
		q.Close()
	}

	hints := &ps.SelectHints{
		Start: timestamp.FromTime(start),
		End:   timestamp.FromTime(end),
		Func:  "series", // There is no series function, this token is used for lookups that don't need samples.
	}
	var set ps.SeriesSet

	if len(matcherSets) > 1 {
		var sets []ps.SeriesSet
		for _, mset := range matcherSets {
			// We need to sort this select results to merge (deduplicate) the series sets later.
			s := q.Select(ctx, true, hints, mset...)
			sets = append(sets, s)
		}
		set = ps.NewMergeSeriesSet(sets, ps.ChainedSeriesMerge)
	} else {
		// At this point at least one match exists.
		set = q.Select(ctx, false, hints, matcherSets[0]...)
	}

	metrics := []labels.Labels{}

	warnings := set.Warnings()

	for set.Next() {
		if err := ctx.Err(); err != nil {
			return apiFuncResult{nil, returnAPIError(err), warnings, closer}
		}
		metrics = append(metrics, set.At().Labels())

		if len(metrics) >= limit {
			warnings.Add(errors.New("results truncated due to limit"))
			return apiFuncResult{metrics, nil, warnings, closer}
		}
	}
	if set.Err() != nil {
		return apiFuncResult{nil, returnAPIError(set.Err()), warnings, closer}
	}

	return apiFuncResult{metrics, nil, warnings, closer}
}

func (api *prometheusAPI) buildInfo(_ *http.Request) apiFuncResult {
	return apiFuncResult{data: map[string]any{
		"status": "success",
		"data": map[string]any{
			"version": "2.24.0",
		},
	}}
}

func extractQueryOpts(r *http.Request) (promql.QueryOpts, error) {
	var duration time.Duration

	if strDuration := r.FormValue("lookback_delta"); strDuration != "" {
		parsedDuration, err := parseDuration(strDuration)
		if err != nil {
			return nil, fmt.Errorf("error parsing lookback delta duration: %w", err)
		}
		duration = parsedDuration
	}

	return promql.NewPrometheusQueryOpts(r.FormValue("stats") == "all", duration), nil
}

func (api *prometheusAPI) respond(w http.ResponseWriter, _ *http.Request, data interface{}) {
	statusMessage := statusSuccess

	resp := &Response{
		Status: statusMessage,
		Data:   data,
	}

	b, err := JSONCodec{}.Encode(resp)
	if err != nil {
		slog.Error("error marshaling response", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if n, err := w.Write(b); err != nil {
		slog.Error("error writing response", "bytesWritten", n, "err", err)
	}
}

func (api *prometheusAPI) respondError(w http.ResponseWriter, apiErr *apiError, data interface{}) {
	b, err := json.Marshal(&Response{
		Status:    statusError,
		ErrorType: apiErr.typ,
		Error:     apiErr.err.Error(),
		Data:      data,
	})
	if err != nil {
		slog.Error("error marshaling json response", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var code int
	switch apiErr.typ {
	case errorBadData:
		code = http.StatusBadRequest
	case errorExec:
		code = http.StatusUnprocessableEntity
	case errorCanceled:
		code = statusClientClosedConnection
	case errorTimeout:
		code = http.StatusServiceUnavailable
	case errorInternal:
		code = http.StatusInternalServerError
	case errorNotFound:
		code = http.StatusNotFound
	case errorNotAcceptable:
		code = http.StatusNotAcceptable
	default:
		code = http.StatusInternalServerError
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if n, err := w.Write(b); err != nil {
		slog.Error("error writing response", "bytesWritten", n, "err", err)
	}
}

func parseTimeParam(r *http.Request, paramName string, defaultValue time.Time) (time.Time, error) {
	val := r.FormValue(paramName)
	if val == "" {
		return defaultValue, nil
	}
	result, err := parseTime(val)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid time value for '%s': %w", paramName, err)
	}
	return result, nil
}

func parseTime(s string) (time.Time, error) {
	if t, err := strconv.ParseFloat(s, 64); err == nil {
		s, ns := math.Modf(t)
		ns = math.Round(ns*1000) / 1000
		return time.Unix(int64(s), int64(ns*float64(time.Second))).UTC(), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}

	// Stdlib's time parser can only handle 4 digit years. As a workaround until
	// that is fixed we want to at least support our own boundary times.
	// Context: https://github.com/prometheus/client_golang/issues/614
	// Upstream issue: https://github.com/golang/go/issues/20555
	switch s {
	case minTimeFormatted:
		return MinTime, nil
	case maxTimeFormatted:
		return MaxTime, nil
	}
	return time.Time{}, fmt.Errorf("cannot parse %q to a valid timestamp", s)
}

func parseDuration(s string) (time.Duration, error) {
	if d, err := strconv.ParseFloat(s, 64); err == nil {
		ts := d * float64(time.Second)
		if ts > float64(math.MaxInt64) || ts < float64(math.MinInt64) {
			return 0, fmt.Errorf("cannot parse %q to a valid duration. It overflows int64", s)
		}
		return time.Duration(ts), nil
	}
	if d, err := model.ParseDuration(s); err == nil {
		return time.Duration(d), nil
	}
	return 0, fmt.Errorf("cannot parse %q to a valid duration", s)
}

func parseMatchersParam(matchers []string) ([][]*labels.Matcher, error) {
	matcherSets, err := parser.ParseMetricSelectors(matchers)
	if err != nil {
		return nil, err
	}

OUTER:
	for _, ms := range matcherSets {
		for _, lm := range ms {
			if lm != nil && !lm.Matches("") {
				continue OUTER
			}
		}
		return nil, errors.New("match[] must contain at least one non-empty matcher")
	}
	return matcherSets, nil
}

func parseLimitParam(limitStr string) (limit int, err error) {
	limit = math.MaxInt
	if limitStr == "" {
		return limit, nil
	}

	limit, err = strconv.Atoi(limitStr)
	if err != nil {
		return limit, err
	}
	if limit <= 0 {
		return limit, errors.New("limit must be positive")
	}

	return limit, nil
}
