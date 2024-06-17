

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

 These endpoints might be supported in the future

- `GET /loki/api/v1/series`
- `GET /loki/api/v1/index/stats`
- `GET /loki/api/v1/index/volume`

 These endpoints will never be supported

- `GET /loki/api/v1/index/volume_range`
- `GET /loki/api/v1/patterns`
- `GET /loki/api/v1/tail`

# Tempo

### Query endpoints

 These endpoints are supported 

- `GET /api/traces/:trace_id`
- `GET /api/search`
- `GET /api/search/tags`
- `GET /api/search/tag/:name/values`