# Multi-stage build producing all Argus Go services in one static image.
# The compose file selects which binary each container runs via `command`.
FROM golang:1.25-alpine AS build
WORKDIR /src

# Cache module downloads.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# Static binaries (CGO off) for a minimal runtime image.
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

FROM alpine:3.20
# TLS roots are required to reach the Binance WebSocket/REST endpoints.
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 argus
COPY --from=build /out/ /usr/local/bin/
# A fresh named volume mounted at /data inherits this ownership on first use,
# so the non-root user can write the audit trail.
RUN mkdir -p /data && chown argus:argus /data
USER argus
# Default to the all-in-one daemon; overridden per service in compose.
EXPOSE 8080 2112 2113
ENTRYPOINT []
CMD ["argusd"]
