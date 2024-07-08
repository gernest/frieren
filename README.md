
# frieren

Ultra  fast alternative to prometheus, loki and tempo for open telemetry
data in (development | testing | staging ) environments.

Test , experiment and verify your open telemetry instrumentation before pushing things 
to production.


# Features

- **Native grafana compatibility**: works with native prometheus. loki and tempo data sources.
- **Single binary,zero dependency**: One `frieren` binary to rule them all.
- **Supports `PromQL` , `LogQL` and `TraceQL`**
- **Standard Ingestion**: Support `otlp` and `otlphttp`. Send data via `gRPC`, `http/json` `http/protobuf`
- **Painless**: Just point your otel collector, or send the data directly via otlp.

```yaml
exporters:
  otlp:
    endpoint: localhost:4317
    tls:
      insecure: true
service:
  pipelines:
    traces:
      exporters: [otlp]
    metrics:
      exporters: [otlp]
    logs:
      exporters: [otlp]
```

- **Familiar API**: query metrics with prometheus api, logs with loki api and traces with tempo api
- **Crazy fast**: We use compressed roaring bitmaps for extremely fast queries.[We use the same technology as Pilosa](https://www.featurebase.com/blog/range-encoded-bitmaps)
- **Realtime**:  If the sample is accepted it is ready to be queried right away.
- **Unlimited cardinality**: We index attributes efficiently
- **Efficient**: numeric data is stored in 2d compressed roaring bitmaps. Blobs are stored
 in content addressable store.

# Prometheus

### Query endpoints

 These endpoints are supported 

- `GET /api/v1/query`
- `GET /api/v1/query_range`
- `GET /api/v1/query_exemplars`
- `GET /api/v1/labels`
- `GET /api/v1/labels/:name/values`
- `GET /api/v1/series`
- `GET /api/v1/metadata`

# Loki

### Query endpoints

 These endpoints are supported 

- `GET /loki/api/v1/query`
- `GET /loki/api/v1/query_range`
- `GET /loki/api/v1/labels`
- `GET /loki/api/v1/labels/:name/values`

# Tempo

### Query endpoints

 These endpoints are supported 

- `GET /api/traces/:trace_id`
- `GET /api/search`
- `GET /api/search/tags`
- `GET /api/search/tag/:name/values`