FROM scratch
LABEL org.opencontainers.image.authors="Geofrey Ernest"
LABEL org.opencontainers.image.source="https://github.com/gernest/frieren"
LABEL org.opencontainers.image.documentation="https://github.com/gernest/frieren"
LABEL org.opencontainers.image.vendor="Geofrey Ernest"
LABEL org.opencontainers.image.description="Crazy fast alternative to prometheus, loki and tempo  for open telemetry data in (development | testing | staging ) environments built on Compressed Roaring Bitmaps"
LABEL org.opencontainers.image.licenses="AGPL-3.0"
ENTRYPOINT ["/frieren"]
COPY frieren /