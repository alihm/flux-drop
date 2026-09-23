FROM golang:1.27.1-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY internal ./internal
COPY cmd ./cmd
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/drop ./cmd/drop && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/drop-init ./cmd/drop-init && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/drop-cluster ./cmd/drop-cluster && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/drop-mcp ./cmd/drop-mcp

FROM nginx:1.30.5-alpine AS app
RUN mkdir -p /data /var/lib/drop-cluster && \
    chown 65534:65534 /data /var/lib/drop-cluster && \
    chmod 0700 /data /var/lib/drop-cluster
COPY --from=build /out/drop /out/drop-init /out/drop-cluster /out/drop-mcp /usr/local/bin/
COPY deploy/nginx.conf /etc/nginx/nginx.conf
COPY deploy/snippets /etc/nginx/drop
ENV DROP_DATA_DIR=/data DROP_ENV=production
USER 65534:65534
EXPOSE 8080 34444 34445 34446
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 CMD ["/usr/local/bin/drop-init", "healthcheck"]
ENTRYPOINT ["/usr/local/bin/drop-init"]
