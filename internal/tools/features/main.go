package main

import (
	"fmt"
	"io"
	"os"
)

func main() {
	o := os.Stdout
	fmt.Fprint(o, "\n# Loki")
	render(o, Loki)
}

type Feature struct {
	API       string
	Supported bool
	Planned   bool
	NotPlaned bool
}

type FeatureSet struct {
	Title    string
	Features []Feature
}

var Loki = []FeatureSet{
	{Title: "Ingest",
		Features: []Feature{
			{
				API:     "POST /loki/api/v1/push",
				Planned: true,
			},
			{
				API:     "POST /otlp/v1/logs",
				Planned: true,
			},
		}},
	{Title: "Query",
		Features: []Feature{
			{
				API:       "GET /loki/api/v1/query",
				Supported: true,
			},
			{
				API:       "GET /loki/api/v1/query_range",
				Supported: true,
			},
			{
				API:       "GET /loki/api/v1/labels",
				Supported: true,
			},
			{
				API:       "GET /loki/api/v1/labels/:name/values",
				Supported: true,
			},
			{
				API:     "GET /loki/api/v1/series",
				Planned: true,
			},
			{
				API:     "GET /loki/api/v1/index/stats",
				Planned: true,
			},
			{
				API:     "GET /loki/api/v1/index/volume",
				Planned: true,
			},
			{
				API:       "GET /loki/api/v1/index/volume_range",
				NotPlaned: true,
			},
			{
				API:       "GET /loki/api/v1/patterns",
				NotPlaned: true,
			},
			{
				API:       "GET /loki/api/v1/tail",
				NotPlaned: true,
			},
		}},
	{Title: "Status"},
	{Title: "Ring"},
	{Title: "Flush/Shutdown"},
	{Title: "Rule"},
}

func render(w io.Writer, set []FeatureSet) {
	var ok, planned, not []Feature
	for _, f := range set {
		fmt.Fprintf(w, "\n\n### %s endpoints", f.Title)
		if len(f.Features) == 0 {
			fmt.Fprint(w, "\n\nAll api calls are not supported")
			continue
		}
		ok = ok[:0]
		planned = planned[:0]
		not = not[:0]
		for _, x := range f.Features {
			if x.Supported {
				ok = append(ok, x)
				continue
			}
			if x.Planned {
				planned = append(planned, x)
				continue
			}
			if x.NotPlaned {
				not = append(not, x)
				continue
			}
		}
		if len(ok) > 0 {
			fmt.Fprint(w, "\n\n These endpoints are supported \n")
			for _, x := range ok {
				fmt.Fprintf(w, "\n- `%s`", x.API)
			}
		}
		if len(planned) > 0 {
			fmt.Fprint(w, "\n\n These endpoints might be supported in the future\n")
			for _, x := range planned {
				fmt.Fprintf(w, "\n- `%s`", x.API)
			}
		}
		if len(not) > 0 {
			fmt.Fprint(w, "\n\n These endpoints will never be supported\n")
			for _, x := range not {
				fmt.Fprintf(w, "\n- `%s`", x.API)
			}
		}
	}
}
