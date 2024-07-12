package audit

import (
	"fmt"
	"log/slog"
	"net/http"
)

func Audit(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("=>", r.URL.Path)
		c := &code{code: http.StatusOK, ResponseWriter: w}
		h.ServeHTTP(c, r)
		slog.Info(r.URL.Path, slog.String("method", r.Method), slog.Int("code", c.code))
	})
}

type code struct {
	http.ResponseWriter
	code int
}

func (c *code) WriteHeader(code int) {
	c.code = code
	c.ResponseWriter.WriteHeader(code)
}
