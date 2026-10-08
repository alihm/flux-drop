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
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/drop-browser ./cmd/drop-browser

FROM nginx:1.30.5-alpine AS app
RUN apk add --no-cache chromium nodejs npm font-noto
WORKDIR /opt/drop-preview
COPY deploy/preview/package.json deploy/preview/package-lock.json ./
RUN npm ci --omit=dev --ignore-scripts && npm cache clean --force
COPY deploy/preview/render.mjs ./
RUN mkdir -p /data /var/lib/drop-cluster && \
    chown 65534:65534 /data /var/lib/drop-cluster && \
    chmod 0700 /data /var/lib/drop-cluster
COPY --from=build /out/drop /out/drop-init /out/drop-cluster /out/drop-mcp /out/drop-browser /usr/local/bin/
COPY deploy/nginx.conf /etc/nginx/nginx.conf
COPY deploy/snippets /etc/nginx/drop
ENV DROP_DATA_DIR=/data DROP_ENV=production
USER 65534:65534
EXPOSE 8080 34444 34445 34446 34447
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 CMD ["/usr/local/bin/drop-init", "healthcheck"]
ENTRYPOINT ["/usr/local/bin/drop-init"]
