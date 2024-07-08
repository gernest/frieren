package http

import "net/http"

type Server struct {
	Trace   TraceCallback
	Logs    LogsCallback
	Metrics MetricsCallback
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/v1/traces":
		handleTraces(w, r, s.Trace)
	case "/v1/metrics":
		handleMetrics(w, r, s.Metrics)
	case "/v1/logs":
		handleLogs(w, r, s.Logs)
	default:
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
	}
}
