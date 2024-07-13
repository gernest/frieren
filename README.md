
# frieren

Ultra  fast alternative to prometheus for open telemetry data in (development | testing | staging ) environments.

Test , experiment and verify your open telemetry instrumentation before pushing things 
to production.

**Designed for rapid prototyping with grafana Explore**

# Features

- **Native grafana compatibility**: works with native prometheus data sources.
- **Single binary,zero dependency**
- **Supports `PromQL`**
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
    metrics:
      exporters: [otlp]
```

- **Familiar API**: exposes prometheus api endpoints
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


## Installation

### Installation script

```
curl -fsSL https://github.com/gernest/frieren/releases/latest/download/install.sh | bash
```

### Container image

```
docker pull ghcr.io/gernest/frieren:latest
```

### Prebult binaries

[See latest release page](https://github.com/gernest/frieren/releases/latest)

## Start server

```
frieren
```


**Usage**
```
NAME:
   frieren - Open Telemetry Storage based on Compressed Roaring Bitmaps

USAGE:
   frieren [global options] [command [command options]] [arguments...]

DESCRIPTION:
   Fast and efficient Open Telemetry storage and query api for (development | testing | staging) environments 

COMMANDS:
   help, h  Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --data value      Path to data directory (default: ".fri-data") [$FRI_DATA]
   --otlp value      host:port for otlp grpc (default: ":4317") [$FRI_OTLP]
   --otlphttp value  host:port for otlp http (default: ":4318") [$FRI_OTLP_HTTP]
   --api value       api exposing prometheus, loki and tempo endpoints (default: ":9000") [$FRI_API]
   --help, -h        show help (default: false)

```