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
	"github.com/gernest/frieren/internal/metrics"
	"github.com/gernest/frieren/internal/self"
	"github.com/gernest/frieren/internal/store"
	"github.com/urfave/cli/v3"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
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
	db *store.Store
	pmetricotlp.UnimplementedGRPCServer
}

func (m *Metrics) Export(ctx context.Context, req pmetricotlp.ExportRequest) (pmetricotlp.ExportResponse, error) {
	_, span := self.Start(ctx, "metrics/Export")
	defer span.End()

	err := metrics.AppendBatch(m.db, metrics.NewBatch(), req.Metrics())
	if err != nil {
		return pmetricotlp.ExportResponse{}, err
	}
	return pmetricotlp.ExportResponse{}, nil
}

type Trace struct {
	ptraceotlp.UnimplementedGRPCServer
}

type Logs struct {
	plogotlp.UnimplementedGRPCServer
}

func Main() *cli.Command {
	return &cli.Command{
		Name: "vectr",
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
				Usage:   "host:port address to listen to otlp collector grpc service",
				Sources: cli.EnvVars("FRI_OTLP"),
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
			httpAPI := c.String("api")
			slog.Info("Initializing server", "data", data, "otlp", otlp, "api", httpAPI)
			ol, err := net.Listen("tcp", otlp)
			if err != nil {
				return err
			}
			defer ol.Close()

			al, err := net.Listen("tcp", httpAPI)
			if err != nil {
				return err
			}
			defer al.Close()

			db, err := store.New(data)
			if err != nil {
				return err
			}
			defer db.Close()

			self.Setup()

			mux := http.NewServeMux()

			// register http api
			api.Add(mux, db)

			gs := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler()))
			defer gs.Stop()

			plogotlp.RegisterGRPCServer(gs, &Logs{})
			pmetricotlp.RegisterGRPCServer(gs, &Metrics{db: db})
			ptraceotlp.RegisterGRPCServer(gs, &Trace{})

			go func() {
				defer cancel()
				slog.Info("starting gRPC otel collector server", "address", otlp)
				err := gs.Serve(ol)
				if err != nil {
					slog.Error("exited grpc service", "err", err)
				}
			}()

			oh := otelhttp.NewHandler(mux, "fri_API")
			go func() {
				defer cancel()
				slog.Info("starting http api server", "address", otlp)
				svr := &http.Server{
					Handler:     oh,
					BaseContext: func(l net.Listener) context.Context { return ctx },
				}
				err := svr.Serve(ol)
				if err != nil {
					slog.Error("exited grpc service", "err", err)
				}
			}()
			<-ctx.Done()
			slog.Info("exiting server")
			return ctx.Err()
		},
	}
}
