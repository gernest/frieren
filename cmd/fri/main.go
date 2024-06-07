package main

import (
	"log"
	"net"

	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
)

func main() {
	ls, err := net.Listen("tcp", ":4317")
	if err != nil {
		log.Fatal(err)
	}
	defer ls.Close()

	svr := grpc.NewServer()
	plogotlp.RegisterGRPCServer(svr, &Logs{})
	pmetricotlp.RegisterGRPCServer(svr, &Metrics{})
	ptraceotlp.RegisterGRPCServer(svr, &Trace{})
	svr.Serve(ls)
}

type Metrics struct {
	pmetricotlp.UnimplementedGRPCServer
}

type Trace struct {
	ptraceotlp.UnimplementedGRPCServer
}

type Logs struct {
	plogotlp.UnimplementedGRPCServer
}
