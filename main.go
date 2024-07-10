package main

import (
	"context"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"

	"github.com/gernest/frieren/internal/api"
	otlphttp "github.com/gernest/frieren/internal/http"
	"github.com/gernest/frieren/internal/logs"
	"github.com/gernest/frieren/internal/metrics"
	"github.com/gernest/frieren/internal/self"
	"github.com/gernest/frieren/internal/traces"
	"github.com/gernest/frieren/internal/util"
	"github.com/urfave/cli/v3"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"google.golang.org/grpc"

	_ "google.golang.org/grpc/encoding/gzip"
)

func main() {
	err := Main().Run(context.Background(), os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

type Metrics struct {
	db *metrics.Store
	pmetricotlp.UnimplementedGRPCServer

	buffer chan *pmetricotlp.ExportRequest
}

func newMetrics(db *metrics.Store) *Metrics {
	return &Metrics{
		db:     db,
		buffer: make(chan *pmetricotlp.ExportRequest, 1<<10),
	}
}

func (m *Metrics) Start(ctx context.Context) {
	slog.Info("starting metrics processing loop")
	for {
		select {
		case <-ctx.Done():
			slog.Info("exiting metrics processing loop")
			return
		case req := <-m.buffer:
			err := m.db.Save(req.Metrics())
			if err != nil {
				slog.Error("failed saving metrics sample", "err", err)
			}
		}
	}
}

func (m *Metrics) Export(ctx context.Context, req pmetricotlp.ExportRequest) (pmetricotlp.ExportResponse, error) {
	m.buffer <- &req
	return pmetricotlp.NewExportResponse(), nil
}

type Trace struct {
	db *traces.Store
	ptraceotlp.UnimplementedGRPCServer
	buffer chan *ptraceotlp.ExportRequest
}

func newTrace(db *traces.Store) *Trace {
	return &Trace{db: db, buffer: make(chan *ptraceotlp.ExportRequest, 1<<10)}
}

func (tr *Trace) Start(ctx context.Context) {
	slog.Info("starting traces processing loop")

	for {
		select {
		case <-ctx.Done():
			slog.Info("exiting traces processing loop")
			return
		case req := <-tr.buffer:
			err := tr.db.Save(req.Traces())
			if err != nil {
				slog.Error("failed saving traces", "err", err)
			}
		}
	}
}

func (tr *Trace) Export(ctx context.Context, req ptraceotlp.ExportRequest) (ptraceotlp.ExportResponse, error) {
	tr.buffer <- &req
	return ptraceotlp.NewExportResponse(), nil
}

type Logs struct {
	db *logs.Store
	plogotlp.UnimplementedGRPCServer
	buffer chan *plogotlp.ExportRequest
}

func newLogs(ldb *logs.Store) *Logs {
	return &Logs{db: ldb, buffer: make(chan *plogotlp.ExportRequest, 1<<10)}
}

func (l *Logs) Start(ctx context.Context) {
	slog.Info("starting logs processing loop")

	for {
		select {
		case <-ctx.Done():
			slog.Info("exiting logs processing loop")
			return
		case req := <-l.buffer:
			err := l.db.Save(req.Logs())
			if err != nil {
				slog.Error("failed saving logs", "err", err)
			}
		}
	}
}

func (l *Logs) Export(ctx context.Context, req plogotlp.ExportRequest) (plogotlp.ExportResponse, error) {
	l.buffer <- &req
	return plogotlp.NewExportResponse(), nil
}

func Main() *cli.Command {
	return &cli.Command{
		Name:        "frieren",
		Usage:       "Open Telemetry Storage based on Compressed Roaring Bitmaps",
		Description: "Fast and efficient Open Telemetry storage and query api for (development | testing | staging) environments ",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "data",
				Usage:   "Path to data directory",
				Value:   ".fri-data",
				Sources: cli.EnvVars("FRI_DATA"),
			},
			&cli.StringFlag{
				Name:    "otlp",
				Value:   ":4317",
				Usage:   "host:port for otlp grpc",
				Sources: cli.EnvVars("FRI_OTLP"),
			},
			&cli.StringFlag{
				Name:    "otlphttp",
				Value:   ":4318",
				Usage:   "host:port for otlp http",
				Sources: cli.EnvVars("FRI_OTLP_HTTP"),
			},
			&cli.StringFlag{
				Name:    "api",
				Value:   ":9000",
				Usage:   "api exposing prometheus, loki and tempo endpoints",
				Sources: cli.EnvVars("FRI_API"),
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
			defer cancel()
			data := c.String("data")
			otlp := c.String("otlp")
			otlpHTTP := c.String("otlphttp")
			httpAPI := c.String("api")
			slog.Info("Initializing server", "data", data, "otlp", otlp, "otlphttp", otlpHTTP, "api", httpAPI)

			otlpListen, err := net.Listen("tcp", otlp)
			if err != nil {
				return err
			}
			defer otlpListen.Close()

			otlpHTTPListen, err := net.Listen("tcp", otlpHTTP)
			if err != nil {
				return err
			}
			defer otlpHTTPListen.Close()

			httpListen, err := net.Listen("tcp", httpAPI)
			if err != nil {
				return err
			}
			defer httpListen.Close()

			mdb, err := metrics.New(data)
			if err != nil {
				return err
			}
			defer mdb.Close()

			ldb, err := logs.New(data)
			if err != nil {
				return err
			}
			defer ldb.Close()

			tdb, err := traces.New(data)
			if err != nil {
				return err
			}
			defer tdb.Close()

			self.Setup(ctx)

			mux := http.NewServeMux()

			// register http api
			api.Add(mux, mdb, tdb, ldb)

			gs := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler()))
			defer gs.Stop()

			ms := newMetrics(mdb)
			go ms.Start(ctx)

			ls := newLogs(ldb)
			go ls.Start(ctx)

			ts := newTrace(tdb)
			go ts.Start(ctx)

			plogotlp.RegisterGRPCServer(gs, ls)
			pmetricotlp.RegisterGRPCServer(gs, ms)
			ptraceotlp.RegisterGRPCServer(gs, ts)

			go func() {
				defer cancel()
				slog.Info("starting otlp grpc", "address", otlp)
				err := gs.Serve(otlpListen)
				if err != nil {
					slog.Error("exited otlp grpc service", "err", err)
				}
			}()
			svc := &otlphttp.Server{
				Trace:   ts.Export,
				Metrics: ms.Export,
				Logs:    ls.Export,
			}
			go func() {
				oh := otelhttp.NewHandler(svc, "fri_otlp_http")
				defer cancel()
				slog.Info("starting otlp http api server", "address", otlpHTTP)
				svr := &http.Server{
					Handler:     oh,
					BaseContext: func(l net.Listener) context.Context { return ctx },
				}
				err := svr.Serve(otlpHTTPListen)
				if err != nil {
					slog.Error("exited otlp http  service", "err", err)
				}
			}()
			go func() {
				oh := otelhttp.NewHandler(mux, "fri_http_api")
				defer cancel()
				slog.Info("starting http api server", "address", httpAPI)
				svr := &http.Server{
					Handler:     oh,
					BaseContext: func(l net.Listener) context.Context { return ctx },
				}
				err := svr.Serve(httpListen)
				if err != nil {
					slog.Error("exited http api service", "err", err)
				}
			}()

			err = runtime.Start()
			if err != nil {
				util.Exit("starring runtime metrics", "err", err)
			}
			<-ctx.Done()
			slog.Info("exiting server")
			return ctx.Err()
		},
	}
}
